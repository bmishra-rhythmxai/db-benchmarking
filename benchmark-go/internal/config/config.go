package config

import "os"

const (
	DBName   = "default"
	User     = "default"
	Password = "strongpassword"
)

// ClickHouse cluster name (must match deployment; used on ON CLUSTER and Distributed table).
const ClickHouseCluster = "dev-cluster"

// ClickHouseStoragePolicy for hl7_messages_local (must exist on ClickHouse server).
func ClickHouseStoragePolicy() string {
	if p := os.Getenv("CLICKHOUSE_STORAGE_POLICY"); p != "" {
		return p
	}
	return "hl7_tiered"
}
