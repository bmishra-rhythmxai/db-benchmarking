package benchmarkgo

import "time"

// Record is (patient_id, message_type, json_message, is_original).
type Record struct {
	PatientID   string
	MessageType string
	JSONMessage string
	IsOriginal  bool
}

// InsertPair is a single queue unit: originals first, then duplicates. The same worker processes both back-to-back on one connection so originals commit before duplicates.
// QueryHint is the prepared hint string (e.g. "/* pgbouncer.database = '...' */ /* pgbouncer.patient_ids = '...' */ ") set by the producer and prepended to the INSERT at the end.
type InsertPair struct {
	Originals  []*Record
	Duplicates []*Record
	QueryHint  string
}

// QueryJob is sent to query workers; nil pointer means QUERY_SENTINEL (stop).
type QueryJob struct {
	MRN        string
	InsertTime time.Time
}

// InsertionSentinel: pass nil *Record to signal end of insertion stream.
