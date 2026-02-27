package postgres

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/db-benchmarking/internal/config"
	"github.com/db-benchmarking/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	createTableSQL = `
CREATE TABLE IF NOT EXISTS hl7_messages (
    fhir_id TEXT,
    rx_patient_id TEXT,
    source TEXT,
    cdc TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    created_by TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    updated_by TEXT,
    load_date TEXT,
    checksum TEXT,
    patient_id TEXT,
    medical_record_number TEXT NOT NULL PRIMARY KEY,
    name_prefix TEXT,
    last_name TEXT,
    first_name TEXT,
    name_suffix TEXT,
    date_of_birth TEXT,
    gender_administrative TEXT,
    fhir_gender_administrative TEXT,
    gender_identity TEXT,
    fhir_gender_identity TEXT,
    marital_status TEXT,
    fhir_marital_status TEXT,
    race_display TEXT,
    fhir_race_display TEXT,
    ethnicity_display TEXT,
    fhir_ethnicity_display TEXT,
    sex_at_birth TEXT,
    is_pregnant TEXT
);
CREATE INDEX IF NOT EXISTS idx_hl7_patient_id ON hl7_messages(patient_id);
`
)

var hl7Columns = []string{
	"fhir_id", "rx_patient_id", "source", "cdc", "created_at", "created_by",
	"updated_at", "updated_by", "load_date", "checksum", "patient_id",
	"medical_record_number", "name_prefix", "last_name", "first_name", "name_suffix",
	"date_of_birth", "gender_administrative", "fhir_gender_administrative",
	"gender_identity", "fhir_gender_identity", "marital_status", "fhir_marital_status",
	"race_display", "fhir_race_display", "ethnicity_display", "fhir_ethnicity_display",
	"sex_at_birth", "is_pregnant",
}

// CreatePool creates a pgx connection pool.
func CreatePool(ctx context.Context, host string, port int, size int) (*pgxpool.Pool, error) {
	connStr := "postgres://" + config.User + ":" + config.Password + "@" + host + ":" + fmtPort(port) + "/" + config.DBName
	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = int32(size)
	cfg.MinConns = int32(size)
	return pgxpool.NewWithConfig(ctx, cfg)
}

func fmtPort(p int) string {
	if p <= 0 {
		return "5432"
	}
	return strconv.Itoa(p)
}

// SetSessionSyncCommit sets synchronous_commit = off for the connection (faster writes).
func SetSessionSyncCommit(ctx context.Context, conn *pgxpool.Conn) error {
	_, err := conn.Exec(ctx, "SET synchronous_commit = off")
	return err
}

// PrewarmPool acquires and releases each connection and sets sync_commit off.
func PrewarmPool(ctx context.Context, pool *pgxpool.Pool, size int) error {
	for i := 0; i < size; i++ {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			return err
		}
		if err := SetSessionSyncCommit(ctx, conn); err != nil {
			conn.Release()
			return err
		}
		conn.Release()
	}
	log.Printf("Prewarmed PostgreSQL connection pool (%d connections)", size)
	return nil
}

// InitSchema creates hl7_messages table if not exists.
func InitSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, createTableSQL)
	if err != nil {
		return err
	}
	log.Println("Table hl7_messages created (PostgreSQL)")
	return nil
}

// rowFromJSON maps producer (patient_id, type, json) to hl7_messages row. now is used for created_at/updated_at.
func rowFromJSON(jsonStr string, now time.Time) ([]interface{}, error) {
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &m); err != nil {
		return nil, err
	}
	get := func(k string) interface{} {
		if v, ok := m[k]; ok {
			return v
		}
		return nil
	}
	createdAt := get("CREATED_AT")
	updatedAt := get("UPDATED_AT")
	if createdAt == nil {
		createdAt = now
	}
	if updatedAt == nil {
		updatedAt = now
	}
	return []interface{}{
		get("FHIR_ID"), get("RX_PATIENT_ID"), get("SOURCE"), get("CDC"),
		createdAt, get("CREATED_BY"), updatedAt, get("UPDATED_BY"),
		get("LOAD_DATE"), get("CHECKSUM"), get("PATIENT_ID"), get("MEDICAL_RECORD_NUMBER"),
		get("NAME_PREFIX"), get("LAST_NAME"), get("FIRST_NAME"), get("NAME_SUFFIX"),
		get("DATE_OF_BIRTH"), get("GENDER_ADMINISTRATIVE"), get("FHIR_GENDER_ADMINISTRATIVE"),
		get("GENDER_IDENTITY"), get("FHIR_GENDER_IDENTITY"), get("MARITAL_STATUS"), get("FHIR_MARITAL_STATUS"),
		get("RACE_DISPLAY"), get("FHIR_RACE_DISPLAY"), get("ETHNICITY_DISPLAY"), get("FHIR_ETHNICITY_DISPLAY"),
		get("SEX_AT_BIRTH"), get("IS_PREGNANT"),
	}, nil
}

// InsertBatch upserts rows into hl7_messages (ON CONFLICT DO UPDATE).
func InsertBatch(ctx context.Context, conn *pgxpool.Conn, rows []worker.RowForDB) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	now := time.Now().UTC()
	updateCols := make([]string, 0, len(hl7Columns)-1)
	for _, c := range hl7Columns {
		if c != "medical_record_number" {
			updateCols = append(updateCols, c)
		}
	}
	setClause := ""
	for i, c := range updateCols {
		if i > 0 {
			setClause += ", "
		}
		setClause += c + " = EXCLUDED." + c
	}
	cols := ""
	for i, c := range hl7Columns {
		if i > 0 {
			cols += ", "
		}
		cols += c
	}
	placeholders := ""
	args := make([]interface{}, 0, len(rows)*len(hl7Columns))
	idx := 1
	for i := range rows {
		if i > 0 {
			placeholders += ", "
		}
		row, err := rowFromJSON(rows[i].JSONMessage, now)
		if err != nil {
			return 0, err
		}
		ph := "("
		for j := 0; j < len(hl7Columns); j++ {
			if j > 0 {
				ph += ", "
			}
			ph += "$" + strconv.Itoa(idx)
			idx++
			args = append(args, row[j])
		}
		ph += ")"
		placeholders += ph
	}
	sql := "INSERT INTO hl7_messages (" + cols + ") VALUES " + placeholders +
		" ON CONFLICT (medical_record_number) DO UPDATE SET " + setClause
	_, err := conn.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

// QueryByPrimaryKey returns rows for the given medical_record_number.
func QueryByPrimaryKey(ctx context.Context, conn *pgxpool.Conn, mrn string) (int, error) {
	var n int
	err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM hl7_messages WHERE medical_record_number = $1", mrn).Scan(&n)
	return n, err
}

// GetMaxPatientCounter returns max patient ordinal from PATIENT_ID 'patient-NNNNNNNNNN', or -1.
func GetMaxPatientCounter(ctx context.Context, conn *pgxpool.Conn) (int, error) {
	var v int64
	err := conn.QueryRow(ctx,
		"SELECT COALESCE(MAX(CAST(SUBSTRING(patient_id FROM 10) AS BIGINT)), -1) FROM hl7_messages WHERE patient_id IS NOT NULL AND patient_id ~ '^patient-[0-9]+$'",
	).Scan(&v)
	if err != nil {
		return -1, err
	}
	return int(v), nil
}
