package producer

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync/atomic"

	"github.com/db-benchmarking/internal/model"
	"github.com/db-benchmarking/internal/patientgen"
)

const patientMessageType = "PATIENT"

// BuildInsertPair builds one InsertPair (one batch split into originals and unique duplicates).
// Call this for each producer in order before starting producer goroutines so batch building order is deterministic.
func BuildInsertPair(batchSize int, patientStartBase int, nextID *atomic.Int64, duplicateRatio float64) *model.InsertPair {
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

// Run produces batches of records and enqueues them until ctx is cancelled.
// initialPair is the pre-built pair for this producer (from BuildInsertPair, called in order before starting goroutines).
// The loop only does: recv -> insert to queue -> send -> build next pair. Between recvCh and sendCh only the queue insertion runs.
func Run(
	ctx context.Context,
	insertionQueue chan *model.InsertPair,
	initialPair *model.InsertPair,
	batchSize int,
	patientStartBase int,
	nextID *atomic.Int64,
	duplicateRatio float64,
	recvCh <-chan struct{},
	sendCh chan<- struct{},
) {
	if batchSize <= 0 {
		return
	}
	pair := initialPair

	for {
		select {
		case <-ctx.Done():
			select {
			case <-recvCh:
				sendCh <- struct{}{}
			default:
			}
			return
		case <-recvCh:
		}
		if ctx.Err() != nil {
			sendCh <- struct{}{}
			return
		}
		// Only insertion to the queue between recvCh and sendCh.
		insertionQueue <- pair
		sendCh <- struct{}{}
		// Build next pair for the next iteration (outside the recv/send window).
		pair = buildInsertPair(batchSize, patientStartBase, nextID, duplicateRatio)
	}
}

func init() {
	_, _ = json.Marshal(patientgen.PatientRecord{})
}
