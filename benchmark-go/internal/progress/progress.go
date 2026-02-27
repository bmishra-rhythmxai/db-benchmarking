package progress

import (
	"context"
	"log"
	"sync"
	"time"
)

const defaultInterval = 5 * time.Second

// Run logs insert and query progress every interval until ctx is done.
func Run(
	ctx context.Context,
	insertedMu *sync.Mutex,
	inserted *InsertedStats,
	queriesMu *sync.Mutex,
	queries *QueryStats,
	interval time.Duration,
) {
	if interval <= 0 {
		interval = defaultInterval
	}
	var prevInserted InsertedStats
	var prevQueries float64
	var prevQueryLatency float64
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		insertedMu.Lock()
		total := inserted.Total
		originals := inserted.Originals
		duplicates := inserted.Duplicates
		totalInsertLatency := inserted.TotalInsertLatencySec
		insertedMu.Unlock()
		queriesMu.Lock()
		q := queries.Count
		totalQueryLatency := queries.TotalLatencySec
		queriesMu.Unlock()

		intervalTotal := int(total - prevInserted.Total)
		intervalOriginals := int(originals - prevInserted.Originals)
		intervalDuplicates := int(duplicates - prevInserted.Duplicates)
		intervalLatency := totalInsertLatency - prevInserted.TotalInsertLatencySec
		prevInserted = InsertedStats{total, originals, duplicates, totalInsertLatency}

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
		prevQueries = q
		prevQueryLatency = totalQueryLatency
		avgLatencyMs := 0.0
		if q > 0 {
			avgLatencyMs = totalQueryLatency / q * 1000
		}
		intervalAvgMs := 0.0
		if intervalQ > 0 {
			intervalAvgMs = intervalQueryLatency / float64(intervalQ) * 1000
		}

		log.Println("---")
		log.Printf("Insert progress (this interval): %d total, %d original, %d duplicate, avg latency %.2f ms",
			intervalTotal, intervalOriginals, intervalDuplicates, intervalAvgInsertMs)
		log.Printf("Query progress (this interval): %d queries, avg latency %.2f ms", intervalQ, intervalAvgMs)
		log.Println("---")
		log.Printf("Insert progress (cumulative): %d total, %d original, %d duplicate, avg latency %.2f ms",
			int(total), int(originals), int(duplicates), cumulativeAvgInsertMs)
		log.Printf("Query progress (cumulative): %.0f queries, avg latency %.2f ms", q, avgLatencyMs)
	}
}

type InsertedStats struct {
	Total                 float64
	Originals             float64
	Duplicates            float64
	TotalInsertLatencySec float64
}

type QueryStats struct {
	Count           float64
	TotalLatencySec float64
}
