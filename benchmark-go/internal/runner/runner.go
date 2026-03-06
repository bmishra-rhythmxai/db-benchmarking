package runner

import (
	"log"
	"sync"
	"time"

	"github.com/db-benchmarking/internal/model"
	"github.com/db-benchmarking/internal/producer"
	"github.com/db-benchmarking/internal/progress"
	"github.com/db-benchmarking/internal/worker"
)

// WorkerCtx is the interface for postgres/clickhouse context (Setup, Teardown, GetMaxPatientCounter, RunQueryWorker).
type WorkerCtx interface {
	Setup(numWorkers, targetRPS int) (worker.InsertBackend, error)
	Teardown()
	GetMaxPatientCounter() (int, error)
	RunQueryWorker(queryQueue <-chan *model.QueryJob, queryCh chan<- progress.QueryUpdate, queriesPerRecord int, queryDelaySec float64, ignoreSelectErrors bool)
}

// RunLoad runs the full load: producers, insert workers, query workers, progress logger.
func RunLoad(
	database string,
	durationSec float64,
	batchSize int,
	batchWaitSec float64,
	workers int,
	patientCount int,
	targetRPS int,
	queriesPerRecord int,
	queryDelaySec float64,
	producerThreads int,
	ignoreSelectErrors bool,
	ctx WorkerCtx,
) {
	// Queue must hold enough for workers to batch; allow backlog so pending can grow when producers outpace workers.
	insertionQueueMax := max3(workers*8, batchSize*workers*2, targetRPS*4)
	if cap := targetRPS * 30; cap > insertionQueueMax {
		insertionQueueMax = cap
	}
	queryQueueMax := max3(workers*4, batchSize*workers*4, targetRPS*4)
	insertionQueue := make(chan *model.Record, insertionQueueMax)
	queryQueue := make(chan *model.QueryJob, queryQueueMax)
	insertStartedCh := make(chan struct{}, 256)
	insertCh := make(chan progress.InsertUpdate, 256)
	queryCh := make(chan progress.QueryUpdate, 256)
	resultCh := make(chan progress.Snapshot, 1)
	pendingCh := make(chan progress.PendingInfo, 1)
	donePending := make(chan struct{})

	runStart := time.Now()
	log.Printf("Connecting to %s (workers=%d, producers=%d, batch_size=%d, batch_wait_sec=%.1f, duration=%.1fs, target_rps=%d, queries_per_record=%d, query_delay=%.0fms)",
		database, workers, producerThreads, batchSize, batchWaitSec, durationSec, targetRPS, queriesPerRecord, queryDelaySec*1000)

	backend, err := ctx.Setup(workers, targetRPS)
	if err != nil {
		log.Fatalf("Setup: %v", err)
	}
	defer ctx.Teardown()

	maxCounter, _ := ctx.GetMaxPatientCounter()
	patientStartBase := max(0, maxCounter+1)
	patientStartStride := 10_000_000
	log.Printf("Producers starting from patient counter %d (max in DB: %d)", patientStartBase, maxCounter)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-donePending:
				close(pendingCh)
				return
			case <-ticker.C:
				select {
				case pendingCh <- progress.PendingInfo{QueueLen: len(insertionQueue), BatchSize: batchSize, QueueCap: insertionQueueMax}:
				default:
				}
			}
		}
	}()

	go progress.Run(insertCh, queryCh, resultCh, insertStartedCh, pendingCh, 5*time.Second)

	var insertWg sync.WaitGroup
	var insertExitWg sync.WaitGroup
	insertExitWg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker.RunInsertWorker(backend, insertionQueue, queryQueue, insertStartedCh, insertCh, batchSize, batchWaitSec, queriesPerRecord, &insertWg, &insertExitWg)
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

	perProducer := targetRPS / producerThreads
	remainder := targetRPS % producerThreads
	var producerWg sync.WaitGroup
	for i := 0; i < producerThreads; i++ {
		rps := perProducer
		if i < remainder {
			rps++
		}
		if rps <= 0 {
			continue
		}
		producerWg.Add(1)
		start := patientStartBase + i*patientStartStride
		go func(rps, start int) {
			defer producerWg.Done()
			producer.Run(durationSec, patientCount, insertionQueue, rps, start, 0, &insertWg)
		}(rps, start)
	}
	producerWg.Wait()

	for i := 0; i < workers; i++ {
		insertWg.Add(1)
		insertionQueue <- nil
	}
	insertWg.Wait()
	insertExitWg.Wait() // wait for all workers to finish their final flush and return before closing channels
	close(donePending)
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
