package runner

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/db-benchmarking/benchmark-go/model"
	"github.com/db-benchmarking/benchmark-go/producer"
	"github.com/db-benchmarking/benchmark-go/progress"
	"github.com/db-benchmarking/benchmark-go/worker"
	"golang.org/x/time/rate"
)

const (
	workerQueueCap   = 10
	progressInterval = 5 * time.Second
)

// Config holds load-run parameters. Used to construct a LoadRunner.
type Config struct {
	Database           string
	DurationSec        float64
	BatchSize          int
	Workers            int
	TargetRPS          int
	QueriesPerRecord   int
	QueryDelaySec      float64
	ProducerThreads    int
	IgnoreSelectErrors bool
	DuplicateRatio     float64
}

// WorkerCtx is the interface for postgres/clickhouse (Setup, Teardown, GetMaxPatientCounter, RunQueryWorker).
type WorkerCtx interface {
	Setup(numWorkers, targetRPS int, queriesPerRecord int) (worker.InsertBackend, error)
	Teardown()
	GetMaxPatientCounter() (int, error)
	RunQueryWorker(workerIndex int, queryQueue <-chan *model.QueryJob, queriesPerRecord int, queryDelaySec float64, ignoreSelectErrors bool)
}

// Router distributes from producer queue to worker queues with rate limiting. Maintains next worker index for round-robin.
type Router struct {
	ProducerQueue <-chan *model.InsertPair
	WorkerQueues   []chan *model.InsertPair
	RateLimiter    *rate.Limiter
	nextIndex      int
}

// NewRouter creates a Router. workerQueues are the per-worker queues to distribute to.
func NewRouter(producerQueue <-chan *model.InsertPair, workerQueues []chan *model.InsertPair, rateLimiter *rate.Limiter) *Router {
	return &Router{
		ProducerQueue: producerQueue,
		WorkerQueues:  workerQueues,
		RateLimiter:   rateLimiter,
	}
}

// Run drains the producer queue, rate-limits, and sends to worker queues round-robin. Closes all worker queues when done.
func (r *Router) Run() {
	for pair := range r.ProducerQueue {
		totalRows := len(pair.Originals) + len(pair.Duplicates)
		if totalRows > 0 && r.RateLimiter != nil {
			_ = r.RateLimiter.WaitN(context.Background(), totalRows)
		}
		idx := r.nextIndex % len(r.WorkerQueues)
		r.nextIndex = (r.nextIndex + 1) % len(r.WorkerQueues)
		r.WorkerQueues[idx] <- pair
		progress.AddInsertStarted(1)
	}
	for i := range r.WorkerQueues {
		close(r.WorkerQueues[i])
	}
}

// LoadRunner holds config, backend context, and runtime state for a load run.
type LoadRunner struct {
	Config    Config
	WorkerCtx WorkerCtx

	// Runtime state (set by Run)
	runStart       time.Time
	producerQueue  chan *model.InsertPair
	queryQueue     chan *model.QueryJob
	workerQueues   []chan *model.InsertPair
	doneCh         chan struct{}
	resultCh       chan progress.Snapshot
	runCtx         context.Context
	cancelRun      context.CancelFunc
	patientStart   int
	nextID         atomic.Int64
	backend        worker.InsertBackend
	triggers       []chan struct{}
	producers      []*producer.Producer
	insertWorkers  []*worker.InsertWorker
	progressReporter *progress.Reporter
}

// NewLoadRunner builds a LoadRunner from config and worker context. Call Run() to execute the load.
func NewLoadRunner(cfg Config, ctx WorkerCtx) *LoadRunner {
	return &LoadRunner{
		Config:    cfg,
		WorkerCtx: ctx,
	}
}

// Run executes the full load: sets up channels and state, starts router, producers, and workers, then waits and logs summary.
func (r *LoadRunner) Run() {
	cfg := &r.Config
	workers := cfg.Workers
	producerThreads := cfg.ProducerThreads

	r.runStart = time.Now()
	producerQueueCap := max3(256, workers*workerQueueCap*2, producerThreads*32)
	queryQueueMax := max3(workers*4, cfg.BatchSize*workers*4, cfg.TargetRPS*4)

	r.producerQueue = make(chan *model.InsertPair, producerQueueCap)
	r.queryQueue = make(chan *model.QueryJob, queryQueueMax)
	r.doneCh = make(chan struct{})
	r.resultCh = make(chan progress.Snapshot, 1)
	r.runCtx, r.cancelRun = context.WithTimeout(context.Background(), time.Duration(cfg.DurationSec*float64(time.Second)))
	defer r.cancelRun()

	r.workerQueues = make([]chan *model.InsertPair, workers)
	for i := 0; i < workers; i++ {
		r.workerQueues[i] = make(chan *model.InsertPair, workerQueueCap)
	}

	log.Printf("Connecting to %s (workers=%d, producers=%d, batch_size=%d, duration=%.1fs, target_rps=%d, queries_per_record=%d, query_delay=%.0fms, duplicate_ratio=%.2f)",
		cfg.Database, workers, producerThreads, cfg.BatchSize, cfg.DurationSec, cfg.TargetRPS, cfg.QueriesPerRecord, cfg.QueryDelaySec*1000, cfg.DuplicateRatio)

	rateLimiter := rate.NewLimiter(rate.Limit(cfg.TargetRPS), cfg.BatchSize)

	var err error
	r.backend, err = r.WorkerCtx.Setup(workers, cfg.TargetRPS, cfg.QueriesPerRecord)
	if err != nil {
		log.Fatalf("Setup: %v", err)
	}
	defer r.WorkerCtx.Teardown()

	maxCounter, _ := r.WorkerCtx.GetMaxPatientCounter()
	r.patientStart = max(0, maxCounter+1)
	r.nextID.Store(int64(r.patientStart))
	log.Printf("Producers using atomic counter starting at %d (max in DB: %d)", r.patientStart, maxCounter)

	r.progressReporter = progress.NewReporter(progressInterval)
	go r.progressReporter.Run(r.doneCh, r.resultCh)

	router := NewRouter(r.producerQueue, r.workerQueues, rateLimiter)
	go router.Run()

	var insertExitWg sync.WaitGroup
	insertExitWg.Add(workers)
	r.insertWorkers = make([]*worker.InsertWorker, workers)
	for i := 0; i < workers; i++ {
		r.insertWorkers[i] = worker.NewInsertWorker(i, r.backend, r.workerQueues[i], r.queryQueue, cfg.QueriesPerRecord, &insertExitWg)
		go r.insertWorkers[i].Run()
	}

	var queryWorkersWg sync.WaitGroup
	runQueryWorkers := cfg.QueriesPerRecord > 0
	if runQueryWorkers {
		for i := 0; i < workers; i++ {
			queryWorkersWg.Add(1)
			workerIndex := i
			go func() {
				defer queryWorkersWg.Done()
				r.WorkerCtx.RunQueryWorker(workerIndex, r.queryQueue, cfg.QueriesPerRecord, cfg.QueryDelaySec, cfg.IgnoreSelectErrors)
			}()
		}
	}

	// Pre-build one InsertPair per producer (order deterministic).
	preBuilt := make([]*model.InsertPair, producerThreads)
	for i := 0; i < producerThreads; i++ {
		preBuilt[i] = producer.BuildInitialPair(cfg.BatchSize, r.patientStart, &r.nextID, cfg.DuplicateRatio)
	}

	r.triggers = make([]chan struct{}, producerThreads)
	for i := range r.triggers {
		r.triggers[i] = make(chan struct{}, 1)
	}
	r.triggers[0] <- struct{}{}

	r.producers = make([]*producer.Producer, producerThreads)
	var producerWg sync.WaitGroup
	for i := 0; i < producerThreads; i++ {
		r.producers[i] = producer.NewProducer(
			i,
			cfg.BatchSize,
			r.patientStart,
			&r.nextID,
			cfg.DuplicateRatio,
			preBuilt[i],
			r.producerQueue,
			r.triggers[i],
			r.triggers[(i+1)%producerThreads],
		)
		producerWg.Add(1)
		go func(p *producer.Producer) {
			defer producerWg.Done()
			p.Run(r.runCtx)
		}(r.producers[i])
	}

	producerWg.Wait()
	close(r.producerQueue)
	insertExitWg.Wait()

	if runQueryWorkers {
		for i := 0; i < workers; i++ {
			r.queryQueue <- nil
		}
		queryWorkersWg.Wait()
	}
	close(r.doneCh)

	snapshot := <-r.resultCh
	r.logSummary(snapshot)
}

func (r *LoadRunner) logSummary(snapshot progress.Snapshot) {
	cfg := &r.Config
	runEnd := time.Now()
	elapsed := runEnd.Sub(r.runStart).Seconds()
	totalInserted := int(snapshot.Inserted.Total)
	originals := int(snapshot.Inserted.Originals)
	duplicates := int(snapshot.Inserted.Duplicates)
	totalInsertLatency := snapshot.Inserted.TotalInsertLatencySec
	insertStatements := int(snapshot.Inserted.InsertStatements)
	queriesFinal := int(snapshot.Queries.Count)
	totalQueryLatency := snapshot.Queries.TotalLatencySec
	queriesFailed := int(snapshot.Queries.FailedCount)

	actualRPS := 0.0
	if elapsed > 0 {
		actualRPS = float64(totalInserted) / elapsed
	}
	avgInsertMs := 0.0
	if totalInserted > 0 {
		avgInsertMs = totalInsertLatency / float64(totalInserted) * 1000
	}
	avgQueryMs := 0.0
	if queriesFinal > 0 {
		avgQueryMs = totalQueryLatency / float64(queriesFinal) * 1000
	}

	log.Printf("Run finished: %d rows inserted (%d original, %d duplicate) in %.2fs (%.1f rows/sec, target %d)",
		totalInserted, originals, duplicates, elapsed, actualRPS, cfg.TargetRPS)
	log.Printf("Database: %s", cfg.Database)
	log.Printf("Duration: %.2fs | Workers: %d | Rows inserted: %d (%d original, %d duplicate) | Insert statements: %d",
		elapsed, cfg.Workers, totalInserted, originals, duplicates, insertStatements)
	log.Printf("Actual insert rate: %.1f rows/sec (target %d)", actualRPS, cfg.TargetRPS)
	if totalInserted > 0 {
		log.Printf("Insert latency: avg %.2f ms/row", avgInsertMs)
	}
	if queriesFinal > 0 {
		actualQueryRPS := 0.0
		if elapsed > 0 {
			actualQueryRPS = float64(queriesFinal) / elapsed
		}
		log.Printf("Actual query rate: %.1f queries/sec", actualQueryRPS)
		log.Printf("Queries: %d executed, %d failed | Query latency: avg %.2f ms per SELECT", queriesFinal, queriesFailed, avgQueryMs)
	}
}

func max3(a, b, c int) int {
	if b > a {
		a = b
	}
	if c > a {
		a = c
	}
	return a
}
