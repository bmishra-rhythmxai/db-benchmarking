package benchmarkgo

import (
	"context"
	"encoding/json"
	"math/rand"
	"strings"
	"sync/atomic"
)

const patientMessageType = "PATIENT"

// Producer holds state for one producer goroutine and produces batches of records.
// Patient ordinals are derived from NextBatchIndex (batch index) so batches are deterministic; no nextID contention.
type Producer struct {
	Index            int
	BatchSize        int
	PatientStartBase int
	NextBatchIndex   *atomic.Int64 // shared; batch index → TargetDB and patient ordinal range
	DuplicateRatio   float64
	ProducerQueue    chan<- *InsertPair
	RecvCh           <-chan struct{}
	SendCh           chan<- struct{}
}

// NewProducer builds a Producer. Pairs are built on each send using batch index for patient ordinals.
func NewProducer(
	index int,
	batchSize int,
	patientStartBase int,
	nextBatchIndex *atomic.Int64,
	duplicateRatio float64,
	producerQueue chan<- *InsertPair,
	recvCh <-chan struct{},
	sendCh chan<- struct{},
) *Producer {
	return &Producer{
		Index:            index,
		BatchSize:        batchSize,
		PatientStartBase: patientStartBase,
		NextBatchIndex:   nextBatchIndex,
		DuplicateRatio:   duplicateRatio,
		ProducerQueue:    producerQueue,
		RecvCh:           recvCh,
		SendCh:           sendCh,
	}
}

// buildInsertPair builds one InsertPair for the given batch index. Patient ordinals are deterministic:
// originals at patientStartBase + batchIndex*batchSize + i; duplicates random in [patientStartBase, patientStartBase + batchIndex*batchSize).
// Batch 0 has no duplicate range so all originals.
func buildInsertPair(batchSize int, patientStartBase int, batchIndex int64, duplicateRatio float64) *InsertPair {
	batch := make([]*Record, 0, batchSize)
	base := patientStartBase + int(batchIndex)*batchSize
	dupEnd := base // exclusive upper bound for duplicate ordinals (batch 0: no duplicates)
	for i := 0; i < batchSize; i++ {
		var ordinal int
		var isOriginal bool
		if rand.Float64() < duplicateRatio && dupEnd > patientStartBase {
			ordinal = patientStartBase + rand.Intn(dupEnd-patientStartBase)
			isOriginal = false
		} else {
			ordinal = base + i
			isOriginal = true
		}
		p := GenerateOnePatient(ordinal, isOriginal)
		jsonMsg, _ := p.ToJSON()
		batch = append(batch, &Record{
			PatientID:   p.PatientID,
			MessageType: patientMessageType,
			JSONMessage: jsonMsg,
			IsOriginal:  p.IsOriginal,
		})
	}
	var originals []*Record
	for _, r := range batch {
		if r != nil && r.IsOriginal {
			originals = append(originals, r)
		}
	}
	seen := make(map[string]struct{})
	var duplicates []*Record
	for _, r := range batch {
		if r == nil || r.IsOriginal {
			continue
		}
		key := r.PatientID + "\x00" + r.MessageType + "\x00" + r.JSONMessage
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		duplicates = append(duplicates, r)
	}
	return &InsertPair{Originals: originals, Duplicates: duplicates}
}

// buildQueryHint builds the single query hint string to prepend to the INSERT (two separate comments: pgbouncer.database, pgbouncer.patient_ids).
// Only originals are included in patient_ids; duplicates are omitted.
func buildQueryHint(batchIndex int64, originals []*Record) string {
	db := "postgres1"
	if batchIndex%2 != 0 {
		db = "postgres2"
	}
	safeDB := strings.ReplaceAll(db, "'", "''")
	prefix := "/* pgbouncer.database = '" + safeDB + "' */ "
	all := make([]string, 0, len(originals))
	for _, r := range originals {
		if r != nil {
			all = append(all, r.PatientID)
		}
	}
	if len(all) > 0 {
		safeIDs := strings.ReplaceAll(strings.Join(all, ","), "'", "''")
		prefix += "/* pgbouncer.patient_ids = '" + safeIDs + "' */ "
	}
	return prefix
}

// Run produces batches and enqueues them until ctx is cancelled.
// Each batch is built from the current batch index (patient ordinals = patientStartBase + batchIndex*batchSize + i).
func (p *Producer) Run(ctx context.Context) {
	if p.BatchSize <= 0 {
		return
	}
	for {
		select {
		case <-ctx.Done():
			select {
			case <-p.RecvCh:
				p.SendCh <- struct{}{}
			default:
			}
			return
		case <-p.RecvCh:
		}
		if ctx.Err() != nil {
			p.SendCh <- struct{}{}
			return
		}
		idx := p.NextBatchIndex.Add(1) - 1
		pair := buildInsertPair(p.BatchSize, p.PatientStartBase, idx, p.DuplicateRatio)
		pair.QueryHint = buildQueryHint(idx, pair.Originals)
		select {
		case <-ctx.Done():
			select {
			case <-p.RecvCh:
				p.SendCh <- struct{}{}
			default:
			}
			return
		case p.ProducerQueue <- pair:
			p.SendCh <- struct{}{}
		}
	}
}

func init() {
	_, _ = json.Marshal(PatientRecord{})
}
