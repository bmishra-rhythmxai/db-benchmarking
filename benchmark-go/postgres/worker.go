package postgres

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/db-benchmarking/benchmark-go"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultHost = "localhost"
const defaultPort = 5432
// When PgbouncerEnabled, connect to pgbouncer (not Postgres directly).
const defaultPgbouncerHost = "pgbouncer"
const defaultPgbouncerPort = 6432

const pgbouncerDB1 = "postgres1"
const pgbouncerDB2 = "postgres2"

// Backend holds the pool and implements benchmarkgo.InsertBackend.
// When pgbouncerMode is true, InsertBatch runs SET pgbouncer.database prepended to INSERT, flip-flopping postgres1/postgres2.
type Backend struct {
	pool            *pgxpool.Pool
	pgbouncerMode   bool
	pgbouncerUseDB1 atomic.Bool
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

// InsertBatch inserts rows using the given connection (must be *pgxpool.Conn). Returns (rowsInserted, statementCount, error).
// When pgbouncerMode is true, runs one round-trip: "SET pgbouncer.database = '...'; INSERT ..." (PgBouncer strips SET, forwards INSERT).
func (b *Backend) InsertBatch(conn interface{}, rows []benchmarkgo.RowForDB) (int, int, error) {
	c, ok := conn.(*pgxpool.Conn)
	if !ok {
		return 0, 0, nil
	}
	ctx := context.Background()
	if b.pgbouncerMode && len(rows) > 0 {
		useDB1 := b.pgbouncerUseDB1.Load()
		b.pgbouncerUseDB1.Store(!useDB1)
		db := pgbouncerDB1
		if !useDB1 {
			db = pgbouncerDB2
		}
		sql, args, err := BuildPgbouncerSetInsertStatement(rows, db)
		if err != nil {
			return 0, 0, err
		}
		if _, err := c.Exec(ctx, sql, args...); err != nil {
			return 0, 0, err
		}
		return len(rows), 1, nil
	}
	n, err := InsertBatch(ctx, c, rows)
	if err != nil {
		return n, 0, err
	}
	return n, 1, nil
}

// Context handles setup/teardown and query workers for PostgreSQL.
type Context struct {
	insertPool        *pgxpool.Pool
	selectPool        *pgxpool.Pool
	PgbouncerEnabled  bool
}

// Setup creates insert pool and optionally a separate select pool. When PgbouncerEnabled, uses one pool (postgres1) and SET pgbouncer.database with INSERT back-to-back.
func (c *Context) Setup(numWorkers, targetRPS int, queriesPerRecord int) (benchmarkgo.InsertBackend, error) {
	if c.insertPool != nil {
		log.Fatal("postgres Setup already called")
	}
	var host string
	var port int
	if c.PgbouncerEnabled {
		host = os.Getenv("POSTGRES_PGBOUNCER_HOST")
		if host == "" {
			host = defaultPgbouncerHost
		}
		port = defaultPgbouncerPort
		if p := os.Getenv("POSTGRES_PGBOUNCER_PORT"); p != "" {
			if v, err := strconv.Atoi(p); err == nil {
				port = v
			}
		}
	} else {
		host = os.Getenv("POSTGRES_HOST")
		if host == "" {
			host = defaultHost
		}
		port = defaultPort
		if p := os.Getenv("POSTGRES_PORT"); p != "" {
			if v, err := strconv.Atoi(p); err == nil {
				port = v
			}
		}
	}
	ctx := context.Background()
	if c.PgbouncerEnabled {
		log.Printf("Creating PostgreSQL connection pool at %s:%d (pgbouncer: postgres1, SET pgbouncer.database + INSERT flip-flop postgres1/postgres2, %d insert)",
			host, port, numWorkers)
		insertPool, err := CreatePoolWithDB(ctx, host, port, numWorkers, pgbouncerDB1)
		if err != nil {
			return nil, err
		}
		c.insertPool = insertPool
		if err := PrewarmPool(ctx, insertPool, numWorkers); err != nil {
			insertPool.Close()
			return nil, err
		}
		c.selectPool, _ = CreatePoolWithDB(ctx, host, port, numWorkers, pgbouncerDB1)
		if c.selectPool != nil {
			_ = PrewarmPool(ctx, c.selectPool, numWorkers)
		}
		if err := InitSchema(ctx, insertPool); err != nil {
			insertPool.Close()
			if c.selectPool != nil {
				c.selectPool.Close()
			}
			return nil, err
		}
		log.Printf("Starting insertions (target %d rows/sec) ...", targetRPS)
		be := &Backend{pool: insertPool, pgbouncerMode: true}
		be.pgbouncerUseDB1.Store(true)
		return be, nil
	}
	log.Printf("Creating PostgreSQL connection pool(s) at %s:%d (%d insert connections)",
		host, port, numWorkers)
	if queriesPerRecord > 0 {
		log.Printf("  + %d select connections for query workers", numWorkers)
	}
	insertPool, err := CreatePool(ctx, host, port, numWorkers)
	if err != nil {
		return nil, err
	}
	c.insertPool = insertPool
	if err := PrewarmPool(ctx, insertPool, numWorkers); err != nil {
		insertPool.Close()
		return nil, err
	}
	if queriesPerRecord > 0 {
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
	}
	if err := InitSchema(ctx, insertPool); err != nil {
		insertPool.Close()
		if c.selectPool != nil {
			c.selectPool.Close()
		}
		return nil, err
	}
	log.Printf("Starting insertions (target %d rows/sec) ...", targetRPS)
	return &Backend{pool: insertPool}, nil
}

// Teardown closes all pools.
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

// GetMaxPatientCounter returns the max patient ordinal in the DB. Uses insert pool when select pool is not initialized.
func (c *Context) GetMaxPatientCounter() (int, error) {
	pool := c.selectPool
	if pool == nil {
		pool = c.insertPool
	}
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		return -1, err
	}
	defer conn.Release()
	return GetMaxPatientCounter(context.Background(), conn)
}

// RunQueryWorker consumes from queryQueue, runs queries_per_record lookups per MRN, reports via benchmarkgo.AddQuery.
// workerIndex is the 0-based index of this query worker.
func (c *Context) RunQueryWorker(
	workerIndex int,
	queryQueue <-chan *benchmarkgo.QueryJob,
	queriesPerRecord int,
	queryDelaySec float64,
	ignoreSelectErrors bool,
) {
	_ = workerIndex // reserved for logging/tracing
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
		latencyMicros := time.Since(t0).Microseconds()
		conn.Release()
		benchmarkgo.AddQuery(int64(queriesPerRecord), latencyMicros, int64(failed))
	}
}
