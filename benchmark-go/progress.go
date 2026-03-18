package benchmarkgo

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

const defaultInterval = 5 * time.Second

// Atomic counters (int64). Latencies stored in microseconds for atomic Add.
var (
	insertTotal         atomic.Int64
	insertOriginals     atomic.Int64
	insertDuplicates    atomic.Int64
	insertLatencyMicros atomic.Int64
	insertStatements    atomic.Int64
	insertStarted       atomic.Int64
	insertPostgres1     atomic.Int64 // rows inserted via pgbouncer.database=postgres1
	insertPostgres2     atomic.Int64 // rows inserted via pgbouncer.database=postgres2
	queryCount          atomic.Int64
	queryLatencyMicros  atomic.Int64
	queryFailed         atomic.Int64
)

func init() {
	// No need to zero - Go zero-inits atomics
}

// AddInsert records an insert batch. Latency is in microseconds.
func AddInsert(total, originals, duplicates, latencyMicros, statements int64) {
	insertTotal.Add(total)
	insertOriginals.Add(originals)
	insertDuplicates.Add(duplicates)
	insertLatencyMicros.Add(latencyMicros)
	insertStatements.Add(statements)
}

// AddInsertStarted records one batch handed to a worker (incoming).
func AddInsertStarted(delta int64) {
	insertStarted.Add(delta)
}

// AddInsertToDB records rows inserted for a specific PgBouncer database (postgres1 or postgres2). No-op if db is empty.
func AddInsertToDB(db string, count int64) {
	switch db {
	case "postgres1":
		insertPostgres1.Add(count)
	case "postgres2":
		insertPostgres2.Add(count)
	}
}

// AddQuery records a query batch. Latency is in microseconds.
func AddQuery(count, latencyMicros, failed int64) {
	queryCount.Add(count)
	queryLatencyMicros.Add(latencyMicros)
	queryFailed.Add(failed)
}

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
	_colorReset  = "\033[0m"
	_colorDim    = "\033[2m"
	_colorCyan   = "\033[36m"
	_colorYellow = "\033[33m"
	_colorGreen  = "\033[32m"
)

// Snapshot is the final aggregated state, sent on resultCh when doneCh is closed.
type Snapshot struct {
	Inserted InsertedStats
	Queries  QueryStats
}

// InsertedStats holds aggregated insert stats.
type InsertedStats struct {
	Total                 float64
	Originals             float64
	Duplicates            float64
	TotalInsertLatencySec float64
	InsertStatements      float64
	Postgres1             float64 // rows inserted via pgbouncer.database=postgres1
	Postgres2             float64 // rows inserted via pgbouncer.database=postgres2
}

// QueryStats holds aggregated query stats.
type QueryStats struct {
	Count           float64
	TotalLatencySec float64
	FailedCount     float64
}

// loadSnapshot reads current atomic counters into a Snapshot (latency from micros to sec).
func loadSnapshot() Snapshot {
	insLat := insertLatencyMicros.Load()
	qLat := queryLatencyMicros.Load()
	return Snapshot{
		Inserted: InsertedStats{
			Total:                 float64(insertTotal.Load()),
			Originals:             float64(insertOriginals.Load()),
			Duplicates:             float64(insertDuplicates.Load()),
			TotalInsertLatencySec: float64(insLat) / 1e6,
			InsertStatements:      float64(insertStatements.Load()),
			Postgres1:             float64(insertPostgres1.Load()),
			Postgres2:             float64(insertPostgres2.Load()),
		},
		Queries: QueryStats{
			Count:           float64(queryCount.Load()),
			TotalLatencySec: float64(qLat) / 1e6,
			FailedCount:     float64(queryFailed.Load()),
		},
	}
}

// Reporter holds state for the progress reporting goroutine and logs insert/query progress every interval.
type Reporter struct {
	Interval          time.Duration
	prevInserted      InsertedStats
	prevInsertStarted int64
	prevPostgres1     int64
	prevPostgres2     int64
	prevQueries       float64
	prevQueryLatency  float64
	prevFailed        float64
}

// NewReporter creates a Reporter with the given log interval. If interval <= 0, defaultInterval is used.
func NewReporter(interval time.Duration) *Reporter {
	if interval <= 0 {
		interval = defaultInterval
	}
	return &Reporter{Interval: interval}
}

// Run runs in the calling goroutine and logs insert/query progress every r.Interval.
// When doneCh is closed, it sends the final Snapshot on resultCh and closes resultCh. resultCh must be buffered (e.g. capacity 1).
func (r *Reporter) Run(doneCh <-chan struct{}, resultCh chan<- Snapshot) {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-doneCh:
			resultCh <- loadSnapshot()
			close(resultCh)
			return
		case <-ticker.C:
			snap := loadSnapshot()
			total := snap.Inserted.Total
			originals := snap.Inserted.Originals
			duplicates := snap.Inserted.Duplicates
			totalInsertLatency := snap.Inserted.TotalInsertLatencySec
			insertStatements := snap.Inserted.InsertStatements
			q := snap.Queries.Count
			totalQueryLatency := snap.Queries.TotalLatencySec
			failed := snap.Queries.FailedCount
			curPostgres1 := insertPostgres1.Load()
			curPostgres2 := insertPostgres2.Load()

			curInsertStarted := insertStarted.Load()
			intervalInsertStarted := int(curInsertStarted - r.prevInsertStarted)
			r.prevInsertStarted = curInsertStarted

			intervalTotal := int(total - r.prevInserted.Total)
			intervalOriginals := int(originals - r.prevInserted.Originals)
			intervalDuplicates := int(duplicates - r.prevInserted.Duplicates)
			intervalLatency := totalInsertLatency - r.prevInserted.TotalInsertLatencySec
			intervalStatements := int(insertStatements - r.prevInserted.InsertStatements)
			r.prevInserted = InsertedStats{total, originals, duplicates, totalInsertLatency, insertStatements, float64(curPostgres1), float64(curPostgres2)}

			intervalAvgInsertMs := 0.0
			if intervalTotal > 0 {
				intervalAvgInsertMs = intervalLatency / float64(intervalTotal) * 1000
			}
			cumulativeAvgInsertMs := 0.0
			if total > 0 {
				cumulativeAvgInsertMs = totalInsertLatency / total * 1000
			}
			intervalPostgres1 := int(curPostgres1 - r.prevPostgres1)
			intervalPostgres2 := int(curPostgres2 - r.prevPostgres2)
			r.prevPostgres1 = curPostgres1
			r.prevPostgres2 = curPostgres2

			intervalQ := int(q - r.prevQueries)
			intervalQueryLatency := totalQueryLatency - r.prevQueryLatency
			intervalFailed := int(failed - r.prevFailed)
			r.prevQueries = q
			r.prevQueryLatency = totalQueryLatency
			r.prevFailed = failed
			avgLatencyMs := 0.0
			if q > 0 {
				avgLatencyMs = totalQueryLatency / q * 1000
			}
			intervalAvgMs := 0.0
			if intervalQ > 0 {
				intervalAvgMs = intervalQueryLatency / float64(intervalQ) * 1000
			}

			colW := 12
			log.Printf("%s---%s", _colorDim, _colorReset)
			log.Println(_colorYellow + "  Insert   " + padLeft("incoming", colW) + padLeft("completed", colW) + " " +
				padLeft("int_tot", colW) + padLeft("int_orig", colW) + padLeft("int_dup", colW) + padLeft("int_avg_ms", colW) + " " +
				padLeft("cum_tot", colW) + padLeft("cum_orig", colW) + padLeft("cum_dup", colW) + padLeft("cum_avg_ms", colW) + _colorReset)
			log.Printf("           %s%*d%s%s%*d%s %s%*d%s%s%*d%s%s%*d%s%s%*.*f%s %s%*d%s%s%*d%s%s%*d%s%s%*.*f%s",
				_colorCyan, colW, intervalInsertStarted, _colorReset,
				_colorCyan, colW, intervalStatements, _colorReset,
				_colorCyan, colW, intervalTotal, _colorReset,
				_colorCyan, colW, intervalOriginals, _colorReset,
				_colorCyan, colW, intervalDuplicates, _colorReset,
				_colorCyan, colW, 2, intervalAvgInsertMs, _colorReset,
				_colorCyan, colW, int(total), _colorReset,
				_colorCyan, colW, int(originals), _colorReset,
				_colorCyan, colW, int(duplicates), _colorReset,
				_colorCyan, colW, 2, cumulativeAvgInsertMs, _colorReset)
			log.Printf("  DB       postgres1: int %s%*d%s cum %s%*d%s   postgres2: int %s%*d%s cum %s%*d%s",
				_colorCyan, colW, intervalPostgres1, _colorReset,
				_colorCyan, colW, int(curPostgres1), _colorReset,
				_colorCyan, colW, intervalPostgres2, _colorReset,
				_colorCyan, colW, int(curPostgres2), _colorReset)
			log.Println(_colorYellow + "  Query    " + padLeft("int_queries", colW) + padLeft("int_failed", colW) + padLeft("int_avg_ms", colW) + " " +
				padLeft("cum_queries", colW) + padLeft("cum_failed", colW) + padLeft("cum_avg_ms", colW) + _colorReset)
			log.Printf("           %s%*d%s%s%*d%s%s%*.*f%s %s%*.0f%s%s%*.0f%s%s%*.*f%s",
				_colorCyan, colW, intervalQ, _colorReset,
				_colorCyan, colW, intervalFailed, _colorReset,
				_colorCyan, colW, 2, intervalAvgMs, _colorReset,
				_colorCyan, colW, q, _colorReset,
				_colorCyan, colW, failed, _colorReset,
				_colorCyan, colW, 2, avgLatencyMs, _colorReset)
		}
	}
}
