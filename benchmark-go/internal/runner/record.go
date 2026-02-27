package runner

// Record is (patient_id, message_type, json_message, is_original).
type Record struct {
	PatientID   string
	MessageType string
	JSONMessage string
	IsOriginal  bool
}

// InsertionSentinel: pass nil *Record to signal end of insertion stream.

