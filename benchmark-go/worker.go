package benchmarkgo

import (
	"encoding/json"
	"log"
	"sync"
	"time"
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
	// InsertBatch returns (rowsInserted, statementCount, error). statementCount is 1 for single-statement inserts, 2 for pipeline (postgres1+postgres2).
	InsertBatch(conn interface{}, rows []RowForDB) (int, int, error)
}

// InsertWorker holds state for one insert worker goroutine. Index identifies this worker (0-based).
type InsertWorker struct {
	Index            int
	Backend          InsertBackend
	WorkerQueue      <-chan *InsertPair
	QueryQueue       chan *QueryJob
	QueriesPerRecord int
	ExitWg           *sync.WaitGroup
}

// NewInsertWorker builds an InsertWorker with the given index and config.
func NewInsertWorker(
	index int,
	backend InsertBackend,
	workerQueue <-chan *InsertPair,
	queryQueue chan *QueryJob,
	queriesPerRecord int,
	exitWg *sync.WaitGroup,
) *InsertWorker {
	return &InsertWorker{
		Index:            index,
		Backend:          backend,
		WorkerQueue:      workerQueue,
		QueryQueue:       queryQueue,
		QueriesPerRecord: queriesPerRecord,
		ExitWg:           exitWg,
	}
}

// Run consumes pairs from the worker queue and inserts until the queue is closed.
func (w *InsertWorker) Run() {
	if w.ExitWg != nil {
		defer w.ExitWg.Done()
	}
	for pair := range w.WorkerQueue {
		w.flushPair(pair)
	}
}

func (w *InsertWorker) flushPair(pair *InsertPair) {
	if pair == nil {
		return
	}
	if len(pair.Originals)+len(pair.Duplicates) == 0 {
		return
	}

	var totalRows, totalOriginals, totalDuplicates, totalStatements int
	var totalLatencySec float64

	// Use a separate connection per batch when we have both originals and duplicates,
	// so we never send two SET pgbouncer.database + INSERT on the same connection
	// back-to-back (PgBouncer rejects SET pgbouncer.database when already in transaction).
	if len(pair.Originals) > 0 {
		conn := w.Backend.GetConn()
		n, nOrig, nDup, stmts, lat := w.insertBatch(conn, pair.Originals)
		w.Backend.ReleaseConn(conn)
		totalRows += n
		totalOriginals += nOrig
		totalDuplicates += nDup
		totalStatements += stmts
		totalLatencySec += lat
	}
	if len(pair.Duplicates) > 0 {
		conn := w.Backend.GetConn()
		n, nOrig, nDup, stmts, lat := w.insertBatch(conn, pair.Duplicates)
		w.Backend.ReleaseConn(conn)
		totalRows += n
		totalOriginals += nOrig
		totalDuplicates += nDup
		totalStatements += stmts
		totalLatencySec += lat
	}

	latencyMicros := int64(totalLatencySec * 1e6)
	stmts64 := int64(totalStatements)
	if stmts64 < 1 {
		stmts64 = 1
	}
	AddInsert(int64(totalRows), int64(totalOriginals), int64(totalDuplicates), latencyMicros, stmts64)
}

func (w *InsertWorker) insertBatch(conn interface{}, batch []*Record) (n int, nOriginals int, nDuplicates int, statements int, latencySec float64) {
	rows := make([]RowForDB, len(batch))
	for i, r := range batch {
		rows[i] = RowForDB{r.PatientID, r.MessageType, r.JSONMessage}
	}
	t0 := time.Now()
	var err error
	n, statements, err = w.Backend.InsertBatch(conn, rows)
	latencySec = time.Since(t0).Seconds()
	if err != nil {
		log.Printf("InsertBatch error: %v", err)
		return n, 0, 0, statements, latencySec
	}
	for _, r := range batch {
		if r.IsOriginal {
			nOriginals++
		}
	}
	nDuplicates = len(batch) - nOriginals
	if w.QueriesPerRecord > 0 {
		insertTime := time.Now()
		for _, mrn := range mrnsFromBatch(batch) {
			w.QueryQueue <- &QueryJob{MRN: mrn, InsertTime: insertTime}
		}
	}
	return n, nOriginals, nDuplicates, statements, latencySec
}

func mrnsFromBatch(batch []*Record) []string {
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
