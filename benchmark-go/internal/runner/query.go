package runner

import "time"

// QueryJob is sent to query workers; nil pointer means QUERY_SENTINEL (stop).
type QueryJob struct {
	MRN        string
	InsertTime time.Time
}
