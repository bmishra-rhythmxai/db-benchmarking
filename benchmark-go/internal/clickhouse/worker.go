package clickhouse

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/db-benchmarking/internal/model"
	"github.com/db-benchmarking/internal/progress"
	"github.com/db-benchmarking/internal/worker"
)

const defaultHost = "clickhouse"
const defaultPort = 9000

// Backend implements worker.InsertBackend using a channel of ClickHouse connections.
type Backend struct {
	ch chan driver.Conn
}

// GetConn acquires a connection from the pool.
func (b *Backend) GetConn() interface{} {
	return <-b.ch
}

// ReleaseConn returns the connection to the pool.
func (b *Backend) ReleaseConn(c interface{}) {
	if conn, ok := c.(driver.Conn); ok {
		b.ch <- conn
	}
}

// InsertBatch inserts rows using the given connection (must be driver.Conn).
func (b *Backend) InsertBatch(conn interface{}, rows []worker.RowForDB) (int, error) {
	c, ok := conn.(driver.Conn)
	if !ok {
		return 0, nil
	}
	return InsertBatch(context.Background(), c, rows)
}

// Context holds the connection pool for setup/teardown and query workers.
type Context struct {
	ch     chan driver.Conn
	conns  []driver.Conn
}

// Setup creates the pool, prewarms, and inits schema.
func (c *Context) Setup(numWorkers, targetRPS int) (worker.InsertBackend, error) {
	if c.ch != nil {
		log.Fatal("clickhouse Setup already called")
	}
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		host = defaultHost
	}
	port := defaultPort
	poolSize := numWorkers * 2
	ctx := context.Background()
	log.Printf("Creating ClickHouse connection pool at %s:%d (%d clients for %d insert + %d query workers) ...",
		host, port, poolSize, numWorkers, numWorkers)
	ch, conns, err := CreatePool(ctx, host, port, poolSize)
	if err != nil {
		return nil, err
	}
	c.ch = ch
	c.conns = conns
	conn := <-ch
	if err := InitSchema(ctx, conn); err != nil {
		ch <- conn
		for _, co := range conns {
			co.Close()
		}
		return nil, err
	}
	ch <- conn
	log.Printf("Starting insertions (target %d rows/sec) ...", targetRPS)
	return &Backend{ch: ch}, nil
}

// Teardown closes all connections.
func (c *Context) Teardown() {
	if c.conns != nil {
		for _, conn := range c.conns {
			conn.Close()
		}
		c.conns = nil
		c.ch = nil
	}
}

// GetMaxPatientCounter returns the max patient ordinal in the DB.
func (c *Context) GetMaxPatientCounter() (int, error) {
	conn := <-c.ch
	defer func() { c.ch <- conn }()
	return GetMaxPatientCounter(context.Background(), conn)
}

// RunQueryWorker consumes from queryQueue and runs queries.
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
		conn := <-c.ch
		t0 := time.Now()
		for i := 0; i < queriesPerRecord; i++ {
			n, _ := QueryByPrimaryKey(context.Background(), conn, job.MRN)
			if n != 1 {
				log.Printf("Query by primary key returned %d rows for MEDICAL_RECORD_NUMBER=%s (expected 1)", n, job.MRN)
			}
		}
		latency := time.Since(t0).Seconds()
		c.ch <- conn
		queriesMu.Lock()
		queries.Count += float64(queriesPerRecord)
		queries.TotalLatencySec += latency
		queriesMu.Unlock()
	}
}
