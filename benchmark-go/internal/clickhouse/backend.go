package clickhouse

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/db-benchmarking/internal/config"
	"github.com/db-benchmarking/internal/worker"
)

// CreatePool creates a channel of ClickHouse connections (each is a separate conn).
func CreatePool(ctx context.Context, host string, port int, size int) (chan driver.Conn, []driver.Conn, error) {
	opts := &clickhouse.Options{
		Addr: []string{host + ":" + fmtPort(port)},
		Auth: clickhouse.Auth{
			Database: config.DBName,
			Username: config.User,
			Password: config.Password,
		},
		DialTimeout: 10 * time.Second,
	}
	ch := make(chan driver.Conn, size)
	var conns []driver.Conn
	for i := 0; i < size; i++ {
		conn, err := clickhouse.Open(opts)
		if err != nil {
			for _, c := range conns {
				c.Close()
			}
			return nil, nil, err
		}
		if err := conn.Ping(ctx); err != nil {
			conn.Close()
			for _, c := range conns {
				c.Close()
			}
			return nil, nil, err
		}
		conns = append(conns, conn)
		ch <- conn
	}
	log.Printf("Prewarmed ClickHouse connection pool (%d clients)", size)
	return ch, conns, nil
}

func fmtPort(p int) string {
	if p <= 0 {
		return "9000"
	}
	return strconv.Itoa(p)
}

// InitSchema creates database and hl7_messages_local + hl7_messages on cluster.
func InitSchema(ctx context.Context, conn driver.Conn) error {
	cluster := config.ClickHouseCluster
	db := config.DBName
	policy := config.ClickHouseStoragePolicy()
	if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+db+" ON CLUSTER '"+cluster+"'"); err != nil {
		return err
	}
	localSQL := `CREATE TABLE IF NOT EXISTS ` + db + `.hl7_messages_local ON CLUSTER '` + cluster + `' (
		FHIR_ID Nullable(String), RX_PATIENT_ID Nullable(String), SOURCE Nullable(String), CDC Nullable(String),
		CREATED_AT DateTime64(3), CREATED_BY Nullable(String), UPDATED_AT DateTime64(3), UPDATED_BY Nullable(String),
		LOAD_DATE Nullable(String), CHECKSUM Nullable(String), PATIENT_ID Nullable(String), MEDICAL_RECORD_NUMBER String,
		NAME_PREFIX Nullable(String), LAST_NAME Nullable(String), FIRST_NAME Nullable(String), NAME_SUFFIX Nullable(String),
		DATE_OF_BIRTH Nullable(String), GENDER_ADMINISTRATIVE Nullable(String), FHIR_GENDER_ADMINISTRATIVE Nullable(String),
		GENDER_IDENTITY Nullable(String), FHIR_GENDER_IDENTITY Nullable(String), MARITAL_STATUS Nullable(String), FHIR_MARITAL_STATUS Nullable(String),
		RACE_DISPLAY Nullable(String), FHIR_RACE_DISPLAY Nullable(String), ETHNICITY_DISPLAY Nullable(String), FHIR_ETHNICITY_DISPLAY Nullable(String),
		SEX_AT_BIRTH Nullable(String), IS_PREGNANT Nullable(String)
	) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/01/hl7_messages_local', '{replica}', UPDATED_AT)
	ORDER BY MEDICAL_RECORD_NUMBER SETTINGS storage_policy = '` + policy + `'`
	if err := conn.Exec(ctx, localSQL); err != nil {
		return err
	}
	distSQL := `CREATE TABLE IF NOT EXISTS ` + db + `.hl7_messages ON CLUSTER '` + cluster + `' (
		FHIR_ID Nullable(String), RX_PATIENT_ID Nullable(String), SOURCE Nullable(String), CDC Nullable(String),
		CREATED_AT DateTime64(3), CREATED_BY Nullable(String), UPDATED_AT DateTime64(3), UPDATED_BY Nullable(String),
		LOAD_DATE Nullable(String), CHECKSUM Nullable(String), PATIENT_ID Nullable(String), MEDICAL_RECORD_NUMBER String,
		NAME_PREFIX Nullable(String), LAST_NAME Nullable(String), FIRST_NAME Nullable(String), NAME_SUFFIX Nullable(String),
		DATE_OF_BIRTH Nullable(String), GENDER_ADMINISTRATIVE Nullable(String), FHIR_GENDER_ADMINISTRATIVE Nullable(String),
		GENDER_IDENTITY Nullable(String), FHIR_GENDER_IDENTITY Nullable(String), MARITAL_STATUS Nullable(String), FHIR_MARITAL_STATUS Nullable(String),
		RACE_DISPLAY Nullable(String), FHIR_RACE_DISPLAY Nullable(String), ETHNICITY_DISPLAY Nullable(String), FHIR_ETHNICITY_DISPLAY Nullable(String),
		SEX_AT_BIRTH Nullable(String), IS_PREGNANT Nullable(String)
	) ENGINE = Distributed('` + cluster + `', '` + db + `', hl7_messages_local, rand())`
	if err := conn.Exec(ctx, distSQL); err != nil {
		return err
	}
	log.Println("Cluster tables hl7_messages created (ClickHouse)")
	return nil
}

func get(m map[string]interface{}, k string) interface{} {
	if v, ok := m[k]; ok {
		return v
	}
	return nil
}

// rowFromJSON maps JSON message to column values for ClickHouse (nullable strings + datetime).
func rowFromJSON(jsonStr string, now time.Time) ([]interface{}, error) {
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &m); err != nil {
		return nil, err
	}
	createdAt := get(m, "CREATED_AT")
	updatedAt := get(m, "UPDATED_AT")
	if createdAt == nil {
		createdAt = now
	}
	if updatedAt == nil {
		updatedAt = now
	}
	return []interface{}{
		get(m, "FHIR_ID"), get(m, "RX_PATIENT_ID"), get(m, "SOURCE"), get(m, "CDC"),
		createdAt, get(m, "CREATED_BY"), updatedAt, get(m, "UPDATED_BY"),
		get(m, "LOAD_DATE"), get(m, "CHECKSUM"), get(m, "PATIENT_ID"), get(m, "MEDICAL_RECORD_NUMBER"),
		get(m, "NAME_PREFIX"), get(m, "LAST_NAME"), get(m, "FIRST_NAME"), get(m, "NAME_SUFFIX"),
		get(m, "DATE_OF_BIRTH"), get(m, "GENDER_ADMINISTRATIVE"), get(m, "FHIR_GENDER_ADMINISTRATIVE"),
		get(m, "GENDER_IDENTITY"), get(m, "FHIR_GENDER_IDENTITY"), get(m, "MARITAL_STATUS"), get(m, "FHIR_MARITAL_STATUS"),
		get(m, "RACE_DISPLAY"), get(m, "FHIR_RACE_DISPLAY"), get(m, "ETHNICITY_DISPLAY"), get(m, "FHIR_ETHNICITY_DISPLAY"),
		get(m, "SEX_AT_BIRTH"), get(m, "IS_PREGNANT"),
	}, nil
}

// InsertBatch inserts rows into default.hl7_messages using PrepareBatch.
func InsertBatch(ctx context.Context, conn driver.Conn, rows []worker.RowForDB) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	now := time.Now().UTC()
	// PrepareBatch expects "INSERT INTO table"; Append() adds rows in table column order.
	insertSQL := `INSERT INTO ` + config.DBName + `.hl7_messages`
	batch, err := conn.PrepareBatch(ctx, insertSQL)
	if err != nil {
		return 0, err
	}
	for _, r := range rows {
		row, err := rowFromJSON(r.JSONMessage, now)
		if err != nil {
			batch.Abort()
			return 0, err
		}
		if err := batch.Append(row...); err != nil {
			batch.Abort()
			return 0, err
		}
	}
	if err := batch.Send(); err != nil {
		return 0, err
	}
	return len(rows), nil
}

// QueryByPrimaryKey returns row count for the given MRN (FINAL).
func QueryByPrimaryKey(ctx context.Context, conn driver.Conn, mrn string) (int, error) {
	row := conn.QueryRow(ctx, "SELECT count() FROM "+config.DBName+".hl7_messages FINAL WHERE MEDICAL_RECORD_NUMBER = $1", mrn)
	var n uint64
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return int(n), nil
}

// GetMaxPatientCounter returns max patient ordinal from PATIENT_ID, or -1.
func GetMaxPatientCounter(ctx context.Context, conn driver.Conn) (int, error) {
	row := conn.QueryRow(ctx, "SELECT COALESCE(MAX(toInt64OrZero(substring(PATIENT_ID, 10))), -1) FROM "+config.DBName+".hl7_messages WHERE PATIENT_ID != ''")
	var n int64
	if err := row.Scan(&n); err != nil {
		return -1, err
	}
	return int(n), nil
}
