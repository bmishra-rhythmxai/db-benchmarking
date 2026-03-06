package postgres

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/db-benchmarking/internal/model"
	"github.com/db-benchmarking/internal/progress"
	"github.com/db-benchmarking/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultHost = "localhost"
const defaultPort = 5432

// Backend holds the pool and implements worker.InsertBackend.
type Backend struct {
	pool *pgxpool.Pool
}

// GetConn acquires a connection from the pool.
func (b *Backend) GetConn() interface{} {
	conn, err := b.pool.Acquire(context.Background())
	if err != nil {
		log.Printf("postgres Acquire: %v", err)
		return nil
	}
	return conn
}

// ReleaseConn returns the connection to the pool.
func (b *Backend) ReleaseConn(c interface{}) {
	if conn, ok := c.(*pgxpool.Conn); ok {
		conn.Release()
	}
}

// InsertBatch inserts rows using the given connection (must be *pgxpool.Conn).
func (b *Backend) InsertBatch(conn interface{}, rows []worker.RowForDB) (int, error) {
	c, ok := conn.(*pgxpool.Conn)
	if !ok {
		return 0, nil
	}
	return InsertBatch(context.Background(), c, rows)
}

// Context handles setup/teardown and query workers for PostgreSQL.
type Context struct {
	insertPool *pgxpool.Pool
	selectPool *pgxpool.Pool
}

// Setup creates separate insert and select pools, prewarms, and inits schema.
func (c *Context) Setup(numWorkers, targetRPS int) (worker.InsertBackend, error) {
	if c.insertPool != nil {
		log.Fatal("postgres Setup already called")
	}
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = defaultHost
	}
	port := defaultPort
	ctx := context.Background()
	log.Printf("Creating PostgreSQL connection pools at %s:%d (%d insert + %d select connections) ...",
		host, port, numWorkers, numWorkers)
	insertPool, err := CreatePool(ctx, host, port, numWorkers)
	if err != nil {
		return nil, err
	}
	c.insertPool = insertPool
	if err := PrewarmPool(ctx, insertPool, numWorkers); err != nil {
		insertPool.Close()
		return nil, err
	}
	selectPool, err := CreatePool(ctx, host, port, numWorkers)
	if err != nil {
		insertPool.Close()
		return nil, err
	}
	c.selectPool = selectPool
	if err := PrewarmPool(ctx, selectPool, numWorkers); err != nil {
		insertPool.Close()
		selectPool.Close()
		return nil, err
	}
	if err := InitSchema(ctx, insertPool); err != nil {
		insertPool.Close()
		selectPool.Close()
		return nil, err
	}
	log.Printf("Starting insertions (target %d rows/sec) ...", targetRPS)
	return &Backend{pool: insertPool}, nil
}

// Teardown closes both pools.
func (c *Context) Teardown() {
	if c.selectPool != nil {
		c.selectPool.Close()
		c.selectPool = nil
	}
	if c.insertPool != nil {
		c.insertPool.Close()
		c.insertPool = nil
	}
}

// GetMaxPatientCounter returns the max patient ordinal in the DB.
func (c *Context) GetMaxPatientCounter() (int, error) {
	conn, err := c.selectPool.Acquire(context.Background())
	if err != nil {
		return -1, err
	}
	defer conn.Release()
	return GetMaxPatientCounter(context.Background(), conn)
}

// RunQueryWorker consumes from queryQueue, runs queries_per_record lookups per MRN, sends query deltas to queryCh.
func (c *Context) RunQueryWorker(
	queryQueue <-chan *model.QueryJob,
	queryCh chan<- progress.QueryUpdate,
	queriesPerRecord int,
	queryDelaySec float64,
	ignoreSelectErrors bool,
) {
	for job := range queryQueue {
		if job == nil {
			return
		}
		if queryDelaySec > 0 {
			deadline := job.InsertTime.Add(time.Duration(queryDelaySec * float64(time.Second)))
			if time.Now().Before(deadline) {
				time.Sleep(time.Until(deadline))
			}
		}
		conn, err := c.selectPool.Acquire(context.Background())
		if err != nil {
			continue
		}
		t0 := time.Now()
		var failed int
		for i := 0; i < queriesPerRecord; i++ {
			n, _ := QueryByPrimaryKey(context.Background(), conn, job.MRN)
			if n != 1 {
				failed++
				if !ignoreSelectErrors {
					log.Printf("Query by primary key returned %d rows for MEDICAL_RECORD_NUMBER=%s (expected 1)", n, job.MRN)
				}
			}
		}
		latency := time.Since(t0).Seconds()
		conn.Release()
		queryCh <- progress.QueryUpdate{
			Count:           float64(queriesPerRecord),
			TotalLatencySec: latency,
			FailedCount:     float64(failed),
		}
	}
}
