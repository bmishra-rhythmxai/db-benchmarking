package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/db-benchmarking/benchmark-go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// BuildInsertStatement returns the INSERT upsert SQL and args for the given rows (for use with Exec or Batch.Queue).
func BuildInsertStatement(rows []benchmarkgo.RowForDB) (sql string, args []interface{}, err error) {
	if len(rows) == 0 {
		return "", nil, nil
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
	args = make([]interface{}, 0, len(rows)*len(hl7Columns))
	idx := 1
	for i := range rows {
		if i > 0 {
			placeholders += ", "
		}
		row, err := rowFromJSON(rows[i].JSONMessage, now)
		if err != nil {
			return "", nil, err
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
	sql = "INSERT INTO hl7_messages (" + cols + ") VALUES " + placeholders +
		" ON CONFLICT (medical_record_number) DO UPDATE SET " + setClause
	return sql, args, nil
}

const (
	hashPartitionModulus = 8
	// citusShardCount is used for create_distributed_table. Set to the number of Citus workers
	// so that shards are placed one per worker (even distribution). Must match worker count.
	citusShardCount = 4

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
    medical_record_number TEXT NOT NULL,
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
    is_pregnant TEXT,
    PRIMARY KEY (medical_record_number)
) PARTITION BY HASH (medical_record_number);
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

// CreatePool creates a pgx connection pool using the default database (postgres).
func CreatePool(ctx context.Context, host string, port int, size int) (*pgxpool.Pool, error) {
	return CreatePoolWithDB(ctx, host, port, size, benchmarkgo.DBName, false)
}

// CreatePoolWithDB creates a pgx connection pool for the given database name (e.g. postgres1, postgres2 for PgBouncer).
// When useSimpleProtocol is true, queries use the simple protocol (no prepared statements), allowing multiple statements in one string (e.g. SET + INSERT for PgBouncer).
func CreatePoolWithDB(ctx context.Context, host string, port int, size int, database string, useSimpleProtocol bool) (*pgxpool.Pool, error) {
	if database == "" {
		database = benchmarkgo.DBName
	}
	connStr := "postgres://" + benchmarkgo.User + ":" + benchmarkgo.Password + "@" + host + ":" + fmtPort(port) + "/" + database
	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = int32(size)
	cfg.MinConns = int32(size)
	if useSimpleProtocol {
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}
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

// InitSchema creates hl7_messages hash-partitioned table if not exists (modulus 8).
// When running on a Citus coordinator, distributes the table by medical_record_number (auto-detected).
func InitSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, createTableSQL); err != nil {
		return err
	}
	for i := 0; i < hashPartitionModulus; i++ {
		partSQL := "CREATE TABLE IF NOT EXISTS hl7_messages_" + strconv.Itoa(i) +
			" PARTITION OF hl7_messages FOR VALUES WITH (MODULUS " + strconv.Itoa(hashPartitionModulus) + ", REMAINDER " + strconv.Itoa(i) + ")"
		if _, err := pool.Exec(ctx, partSQL); err != nil {
			return err
		}
	}
	if _, err := pool.Exec(ctx, "CREATE INDEX IF NOT EXISTS idx_hl7_patient_id ON hl7_messages(patient_id)"); err != nil {
		return err
	}
	log.Printf("Table hl7_messages created with hash partitioning (modulus %d)", hashPartitionModulus)
	// Citus: if extension is present, distribute by medical_record_number with explicit shard_count
	// so that shards are evenly distributed (one shard per worker when citusShardCount == worker count).
	// Hash partition modulus 8 is local to each shard; row placement is hash(mrn) -> shard.
	var hasCitus int
	errExt := pool.QueryRow(ctx, "SELECT 1 FROM pg_extension WHERE extname = 'citus'").Scan(&hasCitus)
	if errExt == nil && hasCitus == 1 {
		var alreadyDist int
		errDist := pool.QueryRow(ctx, "SELECT 1 FROM citus_tables WHERE tablename = 'hl7_messages'").Scan(&alreadyDist)
		if errDist != nil {
			_, errDist = pool.Exec(ctx, "SELECT create_distributed_table('hl7_messages', 'medical_record_number', shard_count => $1)", citusShardCount)
			if errDist != nil {
				var pgErr *pgconn.PgError
				if errors.As(errDist, &pgErr) && pgErr.Code == "42883" {
					// undefined_function — shouldn't happen if extension exists
				} else {
					log.Printf("Citus create_distributed_table: %v", errDist)
				}
			} else {
				log.Printf("Citus: distributed hl7_messages by medical_record_number (shard_count=%d)", citusShardCount)
			}
		}
	}
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
func InsertBatch(ctx context.Context, conn *pgxpool.Conn, rows []benchmarkgo.RowForDB) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	sql, args, err := BuildInsertStatement(rows)
	if err != nil {
		return 0, err
	}
	_, err = conn.Exec(ctx, sql, args...)
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
