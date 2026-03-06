package progress

import (
	"fmt"
	"log"
	"time"
)

const defaultInterval = 5 * time.Second

// padRight returns s padded with spaces on the right to width w.
func padRight(s string, w int) string {
	if len(s) >= w {
		return s
	}
	return s + fmt.Sprintf("%*s", w-len(s), "")
}

// padLeft returns s padded with spaces on the left to width w (right-aligned in column).
func padLeft(s string, w int) string {
	if len(s) >= w {
		return s
	}
	return fmt.Sprintf("%*s", w, s)
}

// ANSI colors for progress output (omit if not a TTY for pipe-friendly logs)
const (
	_colorReset   = "\033[0m"
	_colorDim     = "\033[2m"
	_colorCyan    = "\033[36m"
	_colorYellow  = "\033[33m"
	_colorGreen   = "\033[32m"
)

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
// QueueCap is the insertion queue capacity (max records); pending is capped at QueueCap/BatchSize.
type PendingInfo struct {
	QueueLen  int
	BatchSize int
	QueueCap  int // max records in queue; 0 means unknown
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
	var intervalInsertStarted int // insert statements that started (were queued) this interval
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
			intervalInsertStarted++

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

			pending := 0
			if lastPending.BatchSize > 0 {
				pending = lastPending.QueueLen / lastPending.BatchSize
			}

			// Descriptive column names; int_ = this interval, cum_ = cumulative so "total"/"avg" etc. are unambiguous.
			colW := 12
			log.Printf("%s---%s", _colorDim, _colorReset)
			// Insert: status (incoming first) | interval (int_) | cumulative (cum_)
			log.Println(_colorYellow + "  Insert   " + padLeft("incoming", colW) + padLeft("pending", colW) + padLeft("in_proc", colW) + padLeft("completed", colW) + " " +
				padLeft("int_tot", colW) + padLeft("int_orig", colW) + padLeft("int_dup", colW) + padLeft("int_avg_ms", colW) + " " +
				padLeft("cum_tot", colW) + padLeft("cum_orig", colW) + padLeft("cum_dup", colW) + padLeft("cum_avg_ms", colW) + _colorReset)
			// Data row: 11-char prefix, then 12 columns (incoming, pending, in_proc, completed | int_* | cum_*)
			log.Printf("           %s%*d%s%s%*d%s%s%*d%s%s%*d%s %s%*d%s%s%*d%s%s%*d%s%s%*.*f%s %s%*d%s%s%*d%s%s%*d%s%s%*.*f%s",
				_colorCyan, colW, intervalInsertStarted, _colorReset,
				_colorCyan, colW, pending, _colorReset,
				_colorCyan, colW, inProcess, _colorReset,
				_colorCyan, colW, intervalStatements, _colorReset,
				_colorCyan, colW, intervalTotal, _colorReset,
				_colorCyan, colW, intervalOriginals, _colorReset,
				_colorCyan, colW, intervalDuplicates, _colorReset,
				_colorCyan, colW, 2, intervalAvgInsertMs, _colorReset,
				_colorCyan, colW, int(total), _colorReset,
				_colorCyan, colW, int(originals), _colorReset,
				_colorCyan, colW, int(duplicates), _colorReset,
				_colorCyan, colW, 2, cumulativeAvgInsertMs, _colorReset)
			intervalInsertStarted = 0
			// Query: interval (int_) | cumulative (cum_)
			log.Println(_colorYellow + "  Query    " + padLeft("int_queries", colW) + padLeft("int_failed", colW) + padLeft("int_avg_ms", colW) + " " +
				padLeft("cum_queries", colW) + padLeft("cum_failed", colW) + padLeft("cum_avg_ms", colW) + _colorReset)
			// Data row: 11-char prefix to align with "  Query    ", then 6 columns of width colW
			log.Printf("           %s%*d%s%s%*d%s%s%*.*f%s %s%*.0f%s%s%*.0f%s%s%*.*f%s",
				_colorCyan, colW, intervalQ, _colorReset,
				_colorCyan, colW, intervalFailed, _colorReset,
				_colorCyan, colW, 2, intervalAvgMs, _colorReset,
				_colorCyan, colW, q, _colorReset,
				_colorCyan, colW, failed, _colorReset,
				_colorCyan, colW, 2, avgLatencyMs, _colorReset)
		}
	}

	resultCh <- Snapshot{Inserted: inserted, Queries: queries}
	close(resultCh)
}
