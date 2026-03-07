package worker

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/db-benchmarking/internal/model"
	"github.com/db-benchmarking/internal/progress"
	"golang.org/x/time/rate"
)

// RowForDB is (patient_id, message_type, json_message) for insert.
type RowForDB struct {
	PatientID   string
	MessageType string
	JSONMessage string
}

// InsertBackend is implemented by postgres and clickhouse.
type InsertBackend interface {
	GetConn() interface{}
	ReleaseConn(interface{})
	InsertBatch(conn interface{}, rows []RowForDB) (int, error)
}

func mrnsFromBatch(batch []*model.Record) []string {
	var mrns []string
	for _, rec := range batch {
		if rec == nil {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(rec.JSONMessage), &m); err != nil {
			log.Printf("query queue: could not get MEDICAL_RECORD_NUMBER from record, skipping: %v", err)
			continue
		}
		v, _ := m["MEDICAL_RECORD_NUMBER"]
		s, _ := v.(string)
		if s != "" {
			mrns = append(mrns, s)
		} else {
			log.Printf("query queue: MEDICAL_RECORD_NUMBER is empty, skipping")
		}
	}
	return mrns
}

// RunInsertWorker consumes insertion pairs from insertionQueue. Each pair is processed by the same worker back-to-back (originals then duplicates) on one connection to avoid commit conflicts.
// rateLimiter is shared across workers and limits how many rows/sec are flushed (WaitN before each pair).
// Stops when insertionQueue is closed. exitWg.Done() is called when the worker returns.
func RunInsertWorker(
	backend InsertBackend,
	insertionQueue <-chan *model.InsertPair,
	queryQueue chan *model.QueryJob,
	insertStartedCh chan<- struct{},
	insertCh chan<- progress.InsertUpdate,
	queriesPerRecord int,
	rateLimiter *rate.Limiter,
	exitWg *sync.WaitGroup,
) {
	if exitWg != nil {
		defer exitWg.Done()
	}
	for pair := range insertionQueue {
		flushPair(backend, pair, queryQueue, insertStartedCh, insertCh, queriesPerRecord, rateLimiter)
	}
}

// flushPair processes one InsertPair on a single connection: originals first, then duplicates (same worker, back-to-back commits).
func flushPair(
	backend InsertBackend,
	pair *model.InsertPair,
	queryQueue chan *model.QueryJob,
	insertStartedCh chan<- struct{},
	insertCh chan<- progress.InsertUpdate,
	queriesPerRecord int,
	rateLimiter *rate.Limiter,
) {
	if pair == nil {
		return
	}
	totalRows := len(pair.Originals) + len(pair.Duplicates)
	if totalRows == 0 {
		return
	}
	if rateLimiter != nil {
		_ = rateLimiter.WaitN(context.Background(), totalRows)
	}
	insertStartedCh <- struct{}{} // count incoming after rate limiter allowed the insertion
	conn := backend.GetConn()
	defer backend.ReleaseConn(conn)

	// Originals first (committed first), then duplicates.
	if len(pair.Originals) > 0 {
		insertBatchAndReport(conn, backend, pair.Originals, queryQueue, insertCh, queriesPerRecord)
	}
	if len(pair.Duplicates) > 0 {
		insertBatchAndReport(conn, backend, pair.Duplicates, queryQueue, insertCh, queriesPerRecord)
	}
}

// insertBatchAndReport runs InsertBatch for one batch, sends InsertUpdate, and pushes MRNs to queryQueue.
func insertBatchAndReport(
	conn interface{},
	backend InsertBackend,
	batch []*model.Record,
	queryQueue chan *model.QueryJob,
	insertCh chan<- progress.InsertUpdate,
	queriesPerRecord int,
) {
	rows := make([]RowForDB, len(batch))
	for i, r := range batch {
		rows[i] = RowForDB{r.PatientID, r.MessageType, r.JSONMessage}
	}
	t0 := time.Now()
	n, err := backend.InsertBatch(conn, rows)
	latency := time.Since(t0).Seconds()
	if err != nil {
		log.Printf("InsertBatch error: %v", err)
		return
	}
	nOriginals := 0
	for _, r := range batch {
		if r.IsOriginal {
			nOriginals++
		}
	}
	nDuplicates := len(batch) - nOriginals
	insertCh <- progress.InsertUpdate{
		Total:                 float64(n),
		Originals:             float64(nOriginals),
		Duplicates:            float64(nDuplicates),
		TotalInsertLatencySec: latency,
		InsertStatements:      1,
	}
	if queriesPerRecord > 0 {
		insertTime := time.Now()
		for _, mrn := range mrnsFromBatch(batch) {
			queryQueue <- &model.QueryJob{MRN: mrn, InsertTime: insertTime}
		}
	}
}
