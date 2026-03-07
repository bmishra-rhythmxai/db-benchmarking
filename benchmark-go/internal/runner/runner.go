package runner

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/db-benchmarking/internal/model"
	"github.com/db-benchmarking/internal/producer"
	"github.com/db-benchmarking/internal/progress"
	"github.com/db-benchmarking/internal/worker"
	"golang.org/x/time/rate"
)

// WorkerCtx is the interface for postgres/clickhouse context (Setup, Teardown, GetMaxPatientCounter, RunQueryWorker).
type WorkerCtx interface {
	Setup(numWorkers, targetRPS int) (worker.InsertBackend, error)
	Teardown()
	GetMaxPatientCounter() (int, error)
	RunQueryWorker(queryQueue <-chan *model.QueryJob, queryCh chan<- progress.QueryUpdate, queriesPerRecord int, queryDelaySec float64, ignoreSelectErrors bool)
}

// RunLoad runs the full load: producers enqueue batches in round-robin, workers consume batches and insert.
// Insertion queue holds batches; queue cap is 2*producerThreads. No batch-wait; producers prepare full batches.
func RunLoad(
	database string,
	durationSec float64,
	batchSize int,
	workers int,
	targetRPS int,
	queriesPerRecord int,
	queryDelaySec float64,
	producerThreads int,
	ignoreSelectErrors bool,
	duplicateRatio float64,
	ctx WorkerCtx,
) {
	insertionQueueCap := 2 * producerThreads
	queryQueueMax := max3(workers*4, batchSize*workers*4, targetRPS*4)
	insertionQueue := make(chan *model.InsertPair, insertionQueueCap)
	queryQueue := make(chan *model.QueryJob, queryQueueMax)
	insertStartedCh := make(chan struct{}, 256)
	insertCh := make(chan progress.InsertUpdate, 256)
	queryCh := make(chan progress.QueryUpdate, 256)
	resultCh := make(chan progress.Snapshot, 1)

	runStart := time.Now()
	log.Printf("Connecting to %s (workers=%d, producers=%d, batch_size=%d, duration=%.1fs, target_rps=%d, queries_per_record=%d, query_delay=%.0fms, duplicate_ratio=%.2f)",
		database, workers, producerThreads, batchSize, durationSec, targetRPS, queriesPerRecord, queryDelaySec*1000, duplicateRatio)

	runCtx, cancelRun := context.WithTimeout(context.Background(), time.Duration(durationSec*float64(time.Second)))
	defer cancelRun()

	// Shared rate limiter: targetRPS rows/sec across all workers; burst allows initial batches.
	burst := targetRPS * 2
	if burst < batchSize*workers {
		burst = batchSize * workers
	}
	rateLimiter := rate.NewLimiter(rate.Limit(targetRPS), burst)

	backend, err := ctx.Setup(workers, targetRPS)
	if err != nil {
		log.Fatalf("Setup: %v", err)
	}
	defer ctx.Teardown()

	maxCounter, _ := ctx.GetMaxPatientCounter()
	patientStartBase := max(0, maxCounter+1)
	var nextID atomic.Int64
	nextID.Store(int64(patientStartBase))
	log.Printf("Producers using atomic counter starting at %d (max in DB: %d)", patientStartBase, maxCounter)

	go progress.Run(insertCh, queryCh, resultCh, insertStartedCh, 5*time.Second)

	var insertExitWg sync.WaitGroup
	insertExitWg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker.RunInsertWorker(backend, insertionQueue, queryQueue, insertStartedCh, insertCh, queriesPerRecord, rateLimiter, &insertExitWg)
	}

	var queryWorkersWg sync.WaitGroup
	runQueryWorkers := queriesPerRecord > 0
	if runQueryWorkers {
		for i := 0; i < workers; i++ {
			queryWorkersWg.Add(1)
			go func() {
				defer queryWorkersWg.Done()
				ctx.RunQueryWorker(queryQueue, queryCh, queriesPerRecord, queryDelaySec, ignoreSelectErrors)
			}()
		}
	}

	// Pre-build one InsertPair per producer in order (before any goroutines), so batch building order is deterministic.
	preBuilt := make([]*model.InsertPair, producerThreads)
	for i := 0; i < producerThreads; i++ {
		preBuilt[i] = producer.BuildInsertPair(batchSize, patientStartBase, &nextID, duplicateRatio)
	}

	// Each producer i waits on triggers[i], then signals triggers[(i+1)%N]. Runner seeds triggers[0].
	triggers := make([]chan struct{}, producerThreads)
	for i := range triggers {
		triggers[i] = make(chan struct{}, 1)
	}
	triggers[0] <- struct{}{}
	var producerWg sync.WaitGroup
	for i := 0; i < producerThreads; i++ {
		producerWg.Add(1)
		recvCh := triggers[i]
		sendCh := triggers[(i+1)%producerThreads]
		initialPair := preBuilt[i]
		go func(recv <-chan struct{}, send chan<- struct{}, pair *model.InsertPair) {
			defer producerWg.Done()
			producer.Run(runCtx, insertionQueue, pair, batchSize, patientStartBase, &nextID, duplicateRatio, recv, send)
		}(recvCh, sendCh, initialPair)
	}
	producerWg.Wait()
	close(insertionQueue)
	insertExitWg.Wait()
	close(insertStartedCh)
	close(insertCh)

	if runQueryWorkers {
		for i := 0; i < workers; i++ {
			queryQueue <- nil
		}
		queryWorkersWg.Wait()
	}
	close(queryCh)

	snapshot := <-resultCh
	runEnd := time.Now()
	elapsed := runEnd.Sub(runStart).Seconds()
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
		totalInserted, originals, duplicates, elapsed, actualRPS, targetRPS)

	log.Printf("Database: %s", database)
	log.Printf("Duration: %.2fs | Workers: %d | Rows inserted: %d (%d original, %d duplicate) | Insert statements: %d", elapsed, workers, totalInserted, originals, duplicates, insertStatements)
	log.Printf("Actual insert rate: %.1f rows/sec (target %d)", actualRPS, targetRPS)
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
