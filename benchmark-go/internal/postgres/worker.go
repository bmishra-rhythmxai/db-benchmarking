package postgres

import (
	"context"
	"log"
	"os"
	"sync"
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
	pool *pgxpool.Pool
}

// Setup creates pool, prewarms, and inits schema.
func (c *Context) Setup(numWorkers, targetRPS int) (worker.InsertBackend, error) {
	if c.pool != nil {
		log.Fatal("postgres Setup already called")
	}
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = defaultHost
	}
	port := defaultPort
	poolSize := numWorkers * 2
	ctx := context.Background()
	log.Printf("Creating PostgreSQL connection pool at %s:%d (%d connections for %d insert + %d query workers) ...",
		host, port, poolSize, numWorkers, numWorkers)
	pool, err := CreatePool(ctx, host, port, poolSize)
	if err != nil {
		return nil, err
	}
	c.pool = pool
	if err := PrewarmPool(ctx, pool, poolSize); err != nil {
		pool.Close()
		return nil, err
	}
	if err := InitSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	log.Printf("Starting insertions (target %d rows/sec) ...", targetRPS)
	return &Backend{pool: pool}, nil
}

// Teardown closes the pool.
func (c *Context) Teardown() {
	if c.pool != nil {
		c.pool.Close()
		c.pool = nil
	}
}

// GetMaxPatientCounter returns the max patient ordinal in the DB.
func (c *Context) GetMaxPatientCounter() (int, error) {
	conn, err := c.pool.Acquire(context.Background())
	if err != nil {
		return -1, err
	}
	defer conn.Release()
	return GetMaxPatientCounter(context.Background(), conn)
}

// RunQueryWorker consumes from queryQueue, runs queries_per_record lookups per MRN, updates queries stats.
func (c *Context) RunQueryWorker(
	queryQueue <-chan *model.QueryJob,
	queriesMu *sync.Mutex,
	queries *progress.QueryStats,
	queriesPerRecord int,
	queryDelaySec float64,
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
		conn, err := c.pool.Acquire(context.Background())
		if err != nil {
			continue
		}
		t0 := time.Now()
		for i := 0; i < queriesPerRecord; i++ {
			n, _ := QueryByPrimaryKey(context.Background(), conn, job.MRN)
			if n != 1 {
				log.Printf("Query by primary key returned %d rows for MEDICAL_RECORD_NUMBER=%s (expected 1)", n, job.MRN)
			}
		}
		latency := time.Since(t0).Seconds()
		conn.Release()
		queriesMu.Lock()
		queries.Count += float64(queriesPerRecord)
		queries.TotalLatencySec += latency
		queriesMu.Unlock()
	}
}
