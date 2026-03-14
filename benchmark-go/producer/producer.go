package producer

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync/atomic"

	"github.com/db-benchmarking/benchmark-go/model"
	"github.com/db-benchmarking/benchmark-go/patientgen"
)

const patientMessageType = "PATIENT"

// Producer holds state for one producer goroutine and produces batches of records.
// Index identifies this producer (0-based). Use NewProducer to construct.
type Producer struct {
	Index            int
	BatchSize        int
	PatientStartBase int
	NextID           *atomic.Int64
	DuplicateRatio   float64
	InitialPair      *model.InsertPair
	ProducerQueue    chan<- *model.InsertPair
	RecvCh           <-chan struct{}
	SendCh           chan<- struct{}
}

// NewProducer builds a Producer with the given index and config. initialPair must be pre-built for this producer (e.g. via BuildInitialPair).
func NewProducer(
	index int,
	batchSize int,
	patientStartBase int,
	nextID *atomic.Int64,
	duplicateRatio float64,
	initialPair *model.InsertPair,
	producerQueue chan<- *model.InsertPair,
	recvCh <-chan struct{},
	sendCh chan<- struct{},
) *Producer {
	return &Producer{
		Index:            index,
		BatchSize:        batchSize,
		PatientStartBase: patientStartBase,
		NextID:           nextID,
		DuplicateRatio:   duplicateRatio,
		InitialPair:      initialPair,
		ProducerQueue:    producerQueue,
		RecvCh:           recvCh,
		SendCh:           sendCh,
	}
}

// BuildInitialPair builds one InsertPair for a producer. Call once per producer in order before starting goroutines so batch building order is deterministic.
func BuildInitialPair(batchSize int, patientStartBase int, nextID *atomic.Int64, duplicateRatio float64) *model.InsertPair {
	return buildInsertPair(batchSize, patientStartBase, nextID, duplicateRatio)
}

func buildInsertPair(batchSize int, patientStartBase int, nextID *atomic.Int64, duplicateRatio float64) *model.InsertPair {
	batch := make([]*model.Record, 0, batchSize)
	for len(batch) < batchSize {
		var ordinal int
		var isOriginal bool
		if rand.Float64() < duplicateRatio {
			existingMax := nextID.Load() - 1
			if existingMax < int64(patientStartBase) {
				ordinal = int(nextID.Add(1) - 1)
				isOriginal = true
			} else {
				ordinal = patientStartBase + rand.Intn(int(existingMax)-patientStartBase+1)
				isOriginal = false
			}
		} else {
			ordinal = int(nextID.Add(1) - 1)
			isOriginal = true
		}
		p := patientgen.GenerateOnePatient(ordinal, isOriginal)
		jsonMsg, _ := p.ToJSON()
		batch = append(batch, &model.Record{
			PatientID:   p.PatientID,
			MessageType: patientMessageType,
			JSONMessage: jsonMsg,
			IsOriginal:  p.IsOriginal,
		})
	}
	var originals []*model.Record
	for _, r := range batch {
		if r != nil && r.IsOriginal {
			originals = append(originals, r)
		}
	}
	seen := make(map[string]struct{})
	var duplicates []*model.Record
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
	return &model.InsertPair{Originals: originals, Duplicates: duplicates}
}

// Run produces batches and enqueues them until ctx is cancelled.
// Uses p.InitialPair for the first batch; builds the next pair after each send.
func (p *Producer) Run(ctx context.Context) {
	if p.BatchSize <= 0 {
		return
	}
	pair := p.InitialPair

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
		p.ProducerQueue <- pair
		p.SendCh <- struct{}{}
		pair = buildInsertPair(p.BatchSize, p.PatientStartBase, p.NextID, p.DuplicateRatio)
	}
}

func init() {
	_, _ = json.Marshal(patientgen.PatientRecord{})
}
