package progress

import (
	"log"
	"time"
)

const defaultInterval = 5 * time.Second

// InsertUpdate is a delta sent after each insert batch. The progress goroutine sums these.
type InsertUpdate struct {
	Total                 float64
	Originals             float64
	Duplicates            float64
	TotalInsertLatencySec float64
	InsertStatements      float64
}

// QueryUpdate is a delta sent after each query batch. The progress goroutine sums these.
type QueryUpdate struct {
	Count           float64
	TotalLatencySec float64
	FailedCount     float64
}

// Snapshot is the final aggregated state, sent on resultCh when both insertCh and queryCh are closed.
type Snapshot struct {
	Inserted InsertedStats
	Queries  QueryStats
}

// PendingInfo is sent periodically so progress can show pending insert statements (queue length / batch size).
type PendingInfo struct {
	QueueLen  int
	BatchSize int
}

// InsertedStats holds aggregated insert stats (only read/written by the progress goroutine).
type InsertedStats struct {
	Total                 float64
	Originals             float64
	Duplicates            float64
	TotalInsertLatencySec float64
	InsertStatements      float64
}

// QueryStats holds aggregated query stats (only read/written by the progress goroutine).
type QueryStats struct {
	Count           float64
	TotalLatencySec float64
	FailedCount     float64
}

// Run runs in the calling goroutine and logs insert/query progress every interval.
// It receives deltas on insertCh and queryCh, insertStartedCh for in-flight count, and optionally pendingCh for queue depth.
// When both insertCh and queryCh are closed and drained, it sends the final Snapshot on resultCh
// and closes resultCh. resultCh must be buffered (e.g. capacity 1) or have a reader ready.
func Run(
	insertCh <-chan InsertUpdate,
	queryCh <-chan QueryUpdate,
	resultCh chan<- Snapshot,
	insertStartedCh <-chan struct{},
	pendingCh <-chan PendingInfo,
	interval time.Duration,
) {
	if interval <= 0 {
		interval = defaultInterval
	}
	var inserted InsertedStats
	var queries QueryStats
	var inProcess int
	var lastPending PendingInfo
	var prevInserted InsertedStats
	var prevQueries float64
	var prevQueryLatency float64
	var prevFailed float64
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for insertCh != nil || queryCh != nil {
		select {
		case _, ok := <-insertStartedCh:
			if !ok {
				insertStartedCh = nil
				continue
			}
			inProcess++

		case u, ok := <-insertCh:
			if !ok {
				insertCh = nil
				continue
			}
			inProcess--
			if inProcess < 0 {
				inProcess = 0
			}
			inserted.Total += u.Total
			inserted.Originals += u.Originals
			inserted.Duplicates += u.Duplicates
			inserted.TotalInsertLatencySec += u.TotalInsertLatencySec
			inserted.InsertStatements += u.InsertStatements

		case u, ok := <-queryCh:
			if !ok {
				queryCh = nil
				continue
			}
			queries.Count += u.Count
			queries.TotalLatencySec += u.TotalLatencySec
			queries.FailedCount += u.FailedCount

		case p, ok := <-pendingCh:
			if !ok {
				pendingCh = nil
				continue
			}
			lastPending = p

		case <-ticker.C:
			total := inserted.Total
			originals := inserted.Originals
			duplicates := inserted.Duplicates
			totalInsertLatency := inserted.TotalInsertLatencySec
			insertStatements := inserted.InsertStatements
			q := queries.Count
			totalQueryLatency := queries.TotalLatencySec
			failed := queries.FailedCount

			intervalTotal := int(total - prevInserted.Total)
			intervalOriginals := int(originals - prevInserted.Originals)
			intervalDuplicates := int(duplicates - prevInserted.Duplicates)
			intervalLatency := totalInsertLatency - prevInserted.TotalInsertLatencySec
			intervalStatements := int(insertStatements - prevInserted.InsertStatements)
			prevInserted = InsertedStats{total, originals, duplicates, totalInsertLatency, insertStatements}

			intervalAvgInsertMs := 0.0
			if intervalTotal > 0 {
				intervalAvgInsertMs = intervalLatency / float64(intervalTotal) * 1000
			}
			cumulativeAvgInsertMs := 0.0
			if total > 0 {
				cumulativeAvgInsertMs = totalInsertLatency / total * 1000
			}
			intervalQ := int(q - prevQueries)
			intervalQueryLatency := totalQueryLatency - prevQueryLatency
			intervalFailed := int(failed - prevFailed)
			prevQueries = q
			prevQueryLatency = totalQueryLatency
			prevFailed = failed
			avgLatencyMs := 0.0
			if q > 0 {
				avgLatencyMs = totalQueryLatency / q * 1000
			}
			intervalAvgMs := 0.0
			if intervalQ > 0 {
				intervalAvgMs = intervalQueryLatency / float64(intervalQ) * 1000
			}

			log.Println("---")
			log.Printf("Insert progress (this interval): %d total, %d original, %d duplicate, %d insert statements, avg latency %.2f ms",
				intervalTotal, intervalOriginals, intervalDuplicates, intervalStatements, intervalAvgInsertMs)
			log.Printf("Query progress (this interval): %d queries, %d failed, avg latency %.2f ms", intervalQ, intervalFailed, intervalAvgMs)
			pending := 0
			if lastPending.BatchSize > 0 {
				pending = lastPending.QueueLen / lastPending.BatchSize
			}
			log.Println("---")
			log.Printf("Insert progress (cumulative): %d total, %d original, %d duplicate, %d insert statements, avg latency %.2f ms",
				int(total), int(originals), int(duplicates), int(insertStatements), cumulativeAvgInsertMs)
			log.Printf("Insert status (pending = %d, in process = %d)", pending, inProcess)
			log.Printf("Query progress (cumulative): %.0f queries, %.0f failed, avg latency %.2f ms", q, failed, avgLatencyMs)
		}
	}

	resultCh <- Snapshot{Inserted: inserted, Queries: queries}
	close(resultCh)
}
