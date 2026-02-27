// HL7 messages load benchmark: PostgreSQL or ClickHouse.
// Multithreaded: producer enqueues records at target rate; workers batch and insert in parallel.
package main

import (
	"flag"
	"log"
	"os"

	"github.com/db-benchmarking/internal/clickhouse"
	"github.com/db-benchmarking/internal/postgres"
	"github.com/db-benchmarking/internal/runner"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmsgprefix)
	log.SetOutput(os.Stdout)

	database := flag.String("database", "", "postgres or clickhouse (required)")
	duration := flag.Float64("duration", 60, "Run duration in seconds")
	batchSize := flag.Int("batch-size", 100, "Max rows per batch")
	batchWaitSec := flag.Float64("batch-wait-sec", 1.0, "Max seconds before flushing partial batch")
	workers := flag.Int("workers", 5, "Number of worker goroutines")
	patientCount := flag.Int("patient-count", 1000, "Number of patient IDs to generate")
	rowsPerSecond := flag.Int("rows-per-second", 1000, "Target insert rate (rows/sec)")
	producers := flag.Int("producers", 1, "Number of producer goroutines")
	queriesPerRecord := flag.Int("queries-per-record", 10, "Primary-key queries per inserted record")
	queryDelay := flag.Float64("query-delay", 0, "Fixed delay in ms before querying each record (0 = no delay)")
	flag.Parse()

	if *database != "postgres" && *database != "clickhouse" {
		flag.Usage()
		log.Fatal("--database must be postgres or clickhouse")
	}
	if *workers < 1 {
		log.Fatal("--workers must be >= 1")
	}
	if *producers < 1 {
		log.Fatal("--producers must be >= 1")
	}
	if *batchWaitSec <= 0 {
		log.Fatal("--batch-wait-sec must be > 0")
	}

	queryDelaySec := *queryDelay / 1000

	var ctx runner.WorkerCtx
	if *database == "postgres" {
		ctx = &postgres.Context{}
	} else {
		ctx = &clickhouse.Context{}
	}

	runner.RunLoad(
		*database,
		*duration,
		*batchSize,
		*batchWaitSec,
		*workers,
		*patientCount,
		*rowsPerSecond,
		*queriesPerRecord,
		queryDelaySec,
		*producers,
		ctx,
	)
}
