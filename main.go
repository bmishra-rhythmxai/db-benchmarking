// HL7 messages load benchmark: PostgreSQL or ClickHouse.
// Multithreaded: producer enqueues records at target rate; workers batch and insert in parallel.
package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/db-benchmarking/benchmark-go"
	"github.com/db-benchmarking/benchmark-go/clickhouse"
	"github.com/db-benchmarking/benchmark-go/postgres"
)

// millisWriter prefixes each log line with timestamp in milliseconds (2006/01/02 15:04:05.000).
type millisWriter struct{ w io.Writer }

func (m *millisWriter) Write(p []byte) (n int, err error) {
	prefix := time.Now().Format("2006/01/02 15:04:05.000 ")
	if _, err = m.w.Write([]byte(prefix)); err != nil {
		return 0, err
	}
	return m.w.Write(p)
}

func main() {
	log.SetFlags(0)
	log.SetOutput(&millisWriter{w: os.Stdout})

	database := flag.String("database", "", "postgres or clickhouse (required)")
	pgbouncerEnabled := flag.Bool("pgbouncer-enabled", false, "Use PgBouncer with postgres1/postgres2 aliases and pipeline mode for inserts (postgres only)")
	duration := flag.Float64("duration", 60, "Run duration in seconds")
	batchSize := flag.Int("batch-size", 100, "Rows per batch (producers enqueue full batches)")
	workers := flag.Int("workers", 5, "Number of worker goroutines")
	rowsPerSecond := flag.Int("rows-per-second", 1000, "Target insert rate (rows/sec)")
	producers := flag.Int("producers", 2, "Number of producer goroutines (minimum 2)")
	queriesPerRecord := flag.Int("queries-per-record", 10, "Primary-key queries per inserted record")
	queryDelay := flag.Float64("query-delay", 0, "Fixed delay in ms before querying each record (0 = no delay)")
	ignoreSelectErrors := flag.Bool("ignore-select-errors", false, "Do not log when primary-key query returns != 1 row (avoids console slowdown)")
	duplicateRatio := flag.Float64("duplicate-ratio", 0.25, "Ratio of duplicate records (0-1)")
	flag.Parse()

	if *database != "postgres" && *database != "clickhouse" {
		flag.Usage()
		log.Fatal("--database must be postgres or clickhouse")
	}
	if *workers < 1 {
		log.Fatal("--workers must be >= 1")
	}
	if *producers < 2 {
		log.Fatal("--producers must be >= 2")
	}

	queryDelaySec := *queryDelay / 1000

	var workerCtx benchmarkgo.WorkerCtx
	if *database == "postgres" {
		workerCtx = &postgres.Context{PgbouncerEnabled: *pgbouncerEnabled}
	} else {
		workerCtx = &clickhouse.Context{}
	}

	cfg := benchmarkgo.Config{
		Database:           *database,
		DurationSec:        *duration,
		BatchSize:          *batchSize,
		Workers:            *workers,
		TargetRPS:          *rowsPerSecond,
		QueriesPerRecord:   *queriesPerRecord,
		QueryDelaySec:      queryDelaySec,
		ProducerThreads:    *producers,
		IgnoreSelectErrors: *ignoreSelectErrors,
		DuplicateRatio:     *duplicateRatio,
		PgbouncerEnabled:   *pgbouncerEnabled,
	}
	r := benchmarkgo.NewLoadRunner(cfg, workerCtx)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	r.Run(ctx)
}
