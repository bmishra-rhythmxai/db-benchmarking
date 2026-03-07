package worker

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/db-benchmarking/internal/model"
	"github.com/db-benchmarking/internal/progress"
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

// InsertWorker holds state for one insert worker goroutine. Index identifies this worker (0-based).
type InsertWorker struct {
	Index            int
	Backend          InsertBackend
	WorkerQueue      <-chan *model.InsertPair
	QueryQueue       chan *model.QueryJob
	QueriesPerRecord int
	ExitWg           *sync.WaitGroup
}

// NewInsertWorker builds an InsertWorker with the given index and config.
func NewInsertWorker(
	index int,
	backend InsertBackend,
	workerQueue <-chan *model.InsertPair,
	queryQueue chan *model.QueryJob,
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

func (w *InsertWorker) flushPair(pair *model.InsertPair) {
	if pair == nil {
		return
	}
	if len(pair.Originals)+len(pair.Duplicates) == 0 {
		return
	}
	conn := w.Backend.GetConn()
	defer w.Backend.ReleaseConn(conn)

	var totalRows, totalOriginals, totalDuplicates int
	var totalLatencySec float64

	if len(pair.Originals) > 0 {
		n, nOrig, nDup, lat := w.insertBatch(conn, pair.Originals)
		totalRows += n
		totalOriginals += nOrig
		totalDuplicates += nDup
		totalLatencySec += lat
	}
	if len(pair.Duplicates) > 0 {
		n, nOrig, nDup, lat := w.insertBatch(conn, pair.Duplicates)
		totalRows += n
		totalOriginals += nOrig
		totalDuplicates += nDup
		totalLatencySec += lat
	}

	latencyMicros := int64(totalLatencySec * 1e6)
	progress.AddInsert(int64(totalRows), int64(totalOriginals), int64(totalDuplicates), latencyMicros, 1)
}

func (w *InsertWorker) insertBatch(conn interface{}, batch []*model.Record) (n int, nOriginals int, nDuplicates int, latencySec float64) {
	rows := make([]RowForDB, len(batch))
	for i, r := range batch {
		rows[i] = RowForDB{r.PatientID, r.MessageType, r.JSONMessage}
	}
	t0 := time.Now()
	var err error
	n, err = w.Backend.InsertBatch(conn, rows)
	latencySec = time.Since(t0).Seconds()
	if err != nil {
		log.Printf("InsertBatch error: %v", err)
		return n, 0, 0, latencySec
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
			w.QueryQueue <- &model.QueryJob{MRN: mrn, InsertTime: insertTime}
		}
	}
	return n, nOriginals, nDuplicates, latencySec
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
