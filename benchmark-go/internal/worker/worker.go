package worker

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/db-benchmarking/internal/model"
	"github.com/db-benchmarking/internal/progress"
)

const getPollSec = 5 * time.Millisecond

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

// RunInsertWorker consumes from insertionQueue, batches by batchSize or batchWaitSec, inserts via backend, pushes MRNs to queryQueue.
// Stops when it receives nil (*Record). If wg is non-nil, Done() is called after processing each item (queue drain).
func RunInsertWorker(
	backend InsertBackend,
	insertionQueue <-chan *model.Record,
	queryQueue chan *model.QueryJob,
	insertedMu *sync.Mutex,
	inserted *progress.InsertedStats,
	batchSize int,
	batchWaitSec float64,
	queriesPerRecord int,
	wg *sync.WaitGroup,
) {
	var batch []*model.Record
	batchStart := time.Now()
	for {
		batchElapsed := time.Since(batchStart).Seconds()
		timeout := getPollSec
		if batchWaitSec > batchElapsed {
			waitLeft := time.Duration((batchWaitSec - batchElapsed) * float64(time.Second))
			if waitLeft < timeout {
				timeout = waitLeft
			}
		}
		if timeout < time.Millisecond {
			timeout = time.Millisecond
		}

		select {
		case rec := <-insertionQueue:
			if wg != nil {
				wg.Done()
			}
			if rec == nil {
				if len(batch) > 0 {
					flush(backend, batch, queryQueue, insertedMu, inserted, queriesPerRecord)
				}
				return
			}
			batch = append(batch, rec)
			if len(batch) >= batchSize {
				flush(backend, batch, queryQueue, insertedMu, inserted, queriesPerRecord)
				batch = nil
				batchStart = time.Now()
			}
		case <-time.After(timeout):
			if len(batch) > 0 && time.Since(batchStart).Seconds() >= batchWaitSec {
				flush(backend, batch, queryQueue, insertedMu, inserted, queriesPerRecord)
				batch = nil
				batchStart = time.Now()
			}
		}
	}
}

func flush(
	backend InsertBackend,
	batch []*model.Record,
	queryQueue chan *model.QueryJob,
	insertedMu *sync.Mutex,
	inserted *progress.InsertedStats,
	queriesPerRecord int,
) {
	if len(batch) == 0 {
		return
	}
	conn := backend.GetConn()
	defer backend.ReleaseConn(conn)
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
	insertedMu.Lock()
	inserted.Total += float64(n)
	inserted.Originals += float64(nOriginals)
	inserted.Duplicates += float64(nDuplicates)
	inserted.TotalInsertLatencySec += latency
	insertedMu.Unlock()
	if queriesPerRecord > 0 {
		insertTime := time.Now()
		for _, mrn := range mrnsFromBatch(batch) {
			queryQueue <- &model.QueryJob{MRN: mrn, InsertTime: insertTime}
		}
	}
}
