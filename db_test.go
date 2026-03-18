// db_test.go — straight scenarios for robust PgBouncer testing.
// Covers SELECT, INSERT, TRUNCATE with and without query hint /* pgbouncer.database = dbname */.
//
// Usage: set DATABASE_URL (or PGBOUNCER_URL) then run:
//   go run db_test.go
//
// Example: DATABASE_URL="postgres://user:pass@localhost:6432/postgres?sslmode=disable" go run db_test.go

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const testTable = "pgbouncer_db_test_table"

// hl7InsertSQL matches scripts/psql_prepare_execute_insert.sql: same columns and ON CONFLICT.
// Uses 29 params (medical_record_number passed as param; SQL file uses current_setting('app.mrn', true)).
const hl7InsertSQL = `
INSERT INTO hl7_messages (
  fhir_id, rx_patient_id, source, cdc, created_at, created_by,
  updated_at, updated_by, load_date, checksum, patient_id,
  medical_record_number,
  name_prefix, last_name, first_name, name_suffix,
  date_of_birth, gender_administrative, fhir_gender_administrative,
  gender_identity, fhir_gender_identity, marital_status, fhir_marital_status,
  race_display, fhir_race_display, ethnicity_display, fhir_ethnicity_display,
  sex_at_birth, is_pregnant
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
  $12,
  $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29
)
ON CONFLICT (medical_record_number) DO UPDATE SET
  fhir_id = EXCLUDED.fhir_id,
  rx_patient_id = EXCLUDED.rx_patient_id,
  source = EXCLUDED.source,
  cdc = EXCLUDED.cdc,
  created_at = EXCLUDED.created_at,
  created_by = EXCLUDED.created_by,
  updated_at = EXCLUDED.updated_at,
  updated_by = EXCLUDED.updated_by,
  load_date = EXCLUDED.load_date,
  checksum = EXCLUDED.checksum,
  patient_id = EXCLUDED.patient_id,
  name_prefix = EXCLUDED.name_prefix,
  last_name = EXCLUDED.last_name,
  first_name = EXCLUDED.first_name,
  name_suffix = EXCLUDED.name_suffix,
  date_of_birth = EXCLUDED.date_of_birth,
  gender_administrative = EXCLUDED.gender_administrative,
  fhir_gender_administrative = EXCLUDED.fhir_gender_administrative,
  gender_identity = EXCLUDED.gender_identity,
  fhir_gender_identity = EXCLUDED.fhir_gender_identity,
  marital_status = EXCLUDED.marital_status,
  fhir_marital_status = EXCLUDED.fhir_marital_status,
  race_display = EXCLUDED.race_display,
  fhir_race_display = EXCLUDED.fhir_race_display,
  ethnicity_display = EXCLUDED.ethnicity_display,
  fhir_ethnicity_display = EXCLUDED.fhir_ethnicity_display,
  sex_at_birth = EXCLUDED.sex_at_birth,
  is_pregnant = EXCLUDED.is_pregnant
`

func getConnString() string {
	if s := os.Getenv("DATABASE_URL"); s != "" {
		return s
	}
	if s := os.Getenv("PGBOUNCER_URL"); s != "" {
		return s
	}
	host := os.Getenv("PGHOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("PGPORT")
	if port == "" {
		port = "6432"
	}
	db := os.Getenv("PGDATABASE")
	if db == "" {
		db = "postgres1"
	}
	user := os.Getenv("PGUSER")
	if user == "" {
		user = "default"
	}
	pass := os.Getenv("PGPASSWORD")
	if pass == "" {
		pass = "strongpassword"
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, pass, host, port, db)
}

// testPool is shared across all tests; created in TestMain, closed after m.Run().
var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(getConnString())
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse config: %v\n", err)
		os.Exit(1)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create pool: %v\n", err)
		os.Exit(1)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		fmt.Fprintf(os.Stderr, "ping: %v\n", err)
		os.Exit(1)
	}
	testPool = pool
	code := m.Run()
	testPool.Close()
	os.Exit(code)
}

func getPool(t *testing.T) *pgxpool.Pool {
	if testPool == nil {
		t.Fatal("pool not initialized")
	}
	return testPool
}

func currentDB(ctx context.Context, t *testing.T, pool *pgxpool.Pool) string {
	var name string
	err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&name)
	if err != nil {
		t.Fatalf("current_database: %v", err)
	}
	return name
}

func createTestTable(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id   SERIAL PRIMARY KEY,
			val  TEXT
		)`, testTable))
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
}

// createHL7Table creates hl7_messages with hash partitioning (modulus 8) and patient_id index.
func createHL7Table(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	_, err := pool.Exec(ctx, `DROP TABLE IF EXISTS hl7_messages`)
	if err != nil {
		t.Fatalf("drop hl7_messages: %v", err)
	}
	_, err = pool.Exec(ctx, `
		CREATE TABLE hl7_messages (
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
		) PARTITION BY HASH (medical_record_number)`)
	if err != nil {
		t.Fatalf("create hl7_messages: %v", err)
	}
	for i := 0; i < 8; i++ {
		_, err = pool.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE hl7_messages_%d PARTITION OF hl7_messages FOR VALUES WITH (MODULUS 8, REMAINDER %d)", i, i))
		if err != nil {
			t.Fatalf("create hl7_messages_%d: %v", i, err)
		}
	}
	_, err = pool.Exec(ctx, "CREATE INDEX IF NOT EXISTS idx_hl7_patient_id ON hl7_messages(patient_id)")
	if err != nil {
		t.Fatalf("create idx_hl7_patient_id: %v", err)
	}
}

// hl7InsertArgs returns the same 29 args as scripts/psql_prepare_execute_insert.sql EXECUTE (MRN as param).
func hl7InsertArgs(mrn string) []any {
	return []any{
		"patient-0000000001", "rx-patient-0000000001", "fake-hl7-source-payload", "CDC-EXAMPLE",
		"2025-03-17 12:00:00+00", "system", "2025-03-17 12:00:00+00", "system",
		"2025-03-17", "a1b2c3d4e5", "patient-0000000001",
		mrn,
		"Ms", "Doe", "Jane", nil, "1990-05-15",
		"female", "female", "Female", "female", "Single", "S",
		"White", "2106-3", "Not Hispanic or Latino", "2186-5", "female", "false",
	}
}

// ---- Without query hint ----

func TestSelectSimple_WithoutSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	var n int
	err := pool.QueryRow(ctx, "SELECT 1").Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1, got %d", n)
	}
}

func TestSelectFromTable_WithoutSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	createTestTable(ctx, t, pool)
	var count int
	err := pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", testTable)).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	_ = count
}

func TestInsert_WithoutSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	createHL7Table(ctx, t, pool)
	_, err := pool.Exec(ctx, hl7InsertSQL, hl7InsertArgs("MRN-0000000001")...)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTruncate_WithoutSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	createTestTable(ctx, t, pool)
	_, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", testTable))
	if err != nil {
		t.Fatal(err)
	}
}

func TestSelectInsertTruncate_WithoutSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	createTestTable(ctx, t, pool)
	createHL7Table(ctx, t, pool)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var n int
	if err := tx.QueryRow(ctx, "SELECT 1").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1, got %d", n)
	}
	_, err = tx.Exec(ctx, hl7InsertSQL, hl7InsertArgs("MRN-0000000001")...)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", testTable))
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// Citus-style check (exercises prepared statement / RA_FAKE path).
func TestCitusCheck_WithoutSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	var n int
	err := pool.QueryRow(ctx, "SELECT 1 FROM pg_extension WHERE extname = $1", "citus").Scan(&n)
	if err != nil {
		// No row is ok (Citus not installed)
		if !strings.Contains(err.Error(), "no rows") {
			t.Fatal(err)
		}
		return
	}
	_ = n
}

func TestMultiStatement_WithoutSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	// Use two separate queries; Postgres does not allow multiple statements in one prepared statement (42601).
	var i int
	if err := pool.QueryRow(ctx, "SELECT 1").Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Errorf("expected 1, got %d", i)
	}
	if err := pool.QueryRow(ctx, "SELECT 2").Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 2 {
		t.Errorf("expected 2, got %d", i)
	}
}

// ---- With query hint /* pgbouncer.database = dbname */ ----

func pgbouncerDatabaseHintPrefix(dbName string) string {
	safe := strings.ReplaceAll(dbName, "'", "''")
	return "/* pgbouncer.database = '" + safe + "' */ "
}

func queryWithHint(dbName, sql string) string {
	return pgbouncerDatabaseHintPrefix(dbName) + sql
}

func TestSelectSimple_WithSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	dbName := currentDB(ctx, t, pool)
	var n int
	err := pool.QueryRow(ctx, queryWithHint(dbName, "SELECT 1")).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1, got %d", n)
	}
}

func TestSelectFromTable_WithSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	dbName := currentDB(ctx, t, pool)
	createTestTable(ctx, t, pool)
	var count int
	err := pool.QueryRow(ctx, queryWithHint(dbName, fmt.Sprintf("SELECT COUNT(*) FROM %s", testTable))).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	_ = count
}

func TestInsert_WithSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	dbName := currentDB(ctx, t, pool)
	createHL7Table(ctx, t, pool)
	_, err := pool.Exec(ctx, queryWithHint(dbName, hl7InsertSQL), hl7InsertArgs("MRN-0000000001")...)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTruncate_WithSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	dbName := currentDB(ctx, t, pool)
	createTestTable(ctx, t, pool)
	_, err := pool.Exec(ctx, queryWithHint(dbName, fmt.Sprintf("TRUNCATE TABLE %s", testTable)))
	if err != nil {
		t.Fatal(err)
	}
}

func TestSelectInsertTruncate_WithSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	dbName := currentDB(ctx, t, pool)
	createTestTable(ctx, t, pool)
	createHL7Table(ctx, t, pool)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var n int
	if err := tx.QueryRow(ctx, queryWithHint(dbName, "SELECT 1")).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1, got %d", n)
	}
	_, err = tx.Exec(ctx, queryWithHint(dbName, hl7InsertSQL), hl7InsertArgs("MRN-0000000001")...)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, queryWithHint(dbName, fmt.Sprintf("TRUNCATE TABLE %s", testTable)))
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCitusCheck_WithSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	dbName := currentDB(ctx, t, pool)
	var n int
	err := pool.QueryRow(ctx, queryWithHint(dbName, "SELECT 1 FROM pg_extension WHERE extname = $1"), "citus").Scan(&n)
	if err != nil {
		if !strings.Contains(err.Error(), "no rows") {
			t.Fatal(err)
		}
		return
	}
	_ = n
}

func TestMultiStatement_WithSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	dbName := currentDB(ctx, t, pool)
	// Use two separate queries; Postgres does not allow multiple statements in one prepared statement (42601).
	var i int
	if err := pool.QueryRow(ctx, queryWithHint(dbName, "SELECT 1")).Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Errorf("expected 1, got %d", i)
	}
	if err := pool.QueryRow(ctx, queryWithHint(dbName, "SELECT 2")).Scan(&i); err != nil {
		t.Fatal(err)
	}
	if i != 2 {
		t.Errorf("expected 2, got %d", i)
	}
}

// ---- Combined: hint + query in one (single query string) ----

func TestCombinedSetAndSelect_WithSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	dbName := currentDB(ctx, t, pool)
	q := queryWithHint(dbName, "SELECT 1")
	var n int
	err := pool.QueryRow(ctx, q).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1, got %d", n)
	}
}

func TestCombinedSetAndInsert_WithSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	dbName := currentDB(ctx, t, pool)
	createHL7Table(ctx, t, pool)
	_, err := pool.Exec(ctx, queryWithHint(dbName, hl7InsertSQL), hl7InsertArgs("MRN-0000000001")...)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCombinedSetAndTruncate_WithSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	dbName := currentDB(ctx, t, pool)
	createTestTable(ctx, t, pool)
	q := queryWithHint(dbName, fmt.Sprintf("TRUNCATE TABLE %s", testTable))
	_, err := pool.Exec(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCombinedSetAndCitusCheck_WithSet(t *testing.T) {
	pool := getPool(t)
	ctx := context.Background()
	dbName := currentDB(ctx, t, pool)
	q := queryWithHint(dbName, "SELECT 1 FROM pg_extension WHERE extname = 'citus'")
	var n int
	err := pool.QueryRow(ctx, q).Scan(&n)
	if err != nil {
		if !strings.Contains(err.Error(), "no rows") {
			t.Fatal(err)
		}
		return
	}
	_ = n
}
