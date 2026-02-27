package producer

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/db-benchmarking/internal/patientgen"
	"github.com/db-benchmarking/internal/runner"
)

const patientMessageType = "PATIENT"

const defaultDuplicateRatio = 0.25

// Run enqueues records at targetRPS until durationSec elapses, then sends numSentinels sentinels (nil).
// If wg is non-nil, Add(1) is called before each put so callers can Wait() until queue is drained.
func Run(
	durationSec float64,
	patientCount int,
	insertionQueue chan *runner.Record,
	targetRPS int,
	patientStart int,
	numSentinels int,
	wg *sync.WaitGroup,
) {
	put := func(rec *runner.Record) {
		if wg != nil {
			wg.Add(1)
		}
		insertionQueue <- rec
	}
	if targetRPS <= 0 {
		for i := 0; i < numSentinels; i++ {
			put(nil)
		}
		return
	}
	intervalSec := 1.0 / float64(targetRPS)
	start := time.Now()
	deadline := start.Add(time.Duration(durationSec * float64(time.Second)))
	nextPutAt := start
	patientRecords := make([]patientgen.PatientRecord, 0)
	nUnique := max(1, int(float64(patientCount)*(1-defaultDuplicateRatio)))

	for time.Now().Before(deadline) {
		now := time.Now()
		if now.Before(nextPutAt) {
			time.Sleep(time.Until(nextPutAt))
			continue
		}
		if len(patientRecords) == 0 {
			patientRecords = patientgen.GenerateBulkPatients(patientStart, patientCount, defaultDuplicateRatio)
			patientStart += nUnique
		}
		p := patientRecords[0]
		patientRecords = patientRecords[1:]
		jsonMsg, _ := p.ToJSON()
		rec := &runner.Record{
			PatientID:   p.PatientID,
			MessageType: patientMessageType,
			JSONMessage: jsonMsg,
			IsOriginal:  p.IsOriginal,
		}
		put(rec)
		nextPutAt = nextPutAt.Add(time.Duration(intervalSec * float64(time.Second)))
		if nextPutAt.Before(now) {
			nextPutAt = now.Add(time.Duration(intervalSec * float64(time.Second)))
		}
	}

	for i := 0; i < numSentinels; i++ {
		put(nil)
	}
}

func init() {
	_ = json.Marshal(patientgen.PatientRecord{})
}
