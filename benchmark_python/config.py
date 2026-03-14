"""Shared configuration for the load driver."""

import os

DB_NAME = "default"
USER = "default"
PASSWORD = "strongpassword"

# ClickHouse cluster name (must match deployment; used on ON CLUSTER and Distributed table)
CLICKHOUSE_CLUSTER = "dev-cluster"

# Storage policy for hl7_messages_local (must exist on ClickHouse server, e.g. deployments/clickhouse-storage-config.yaml).
CLICKHOUSE_STORAGE_POLICY = os.environ.get("CLICKHOUSE_STORAGE_POLICY", "hl7_tiered")

# Sentinels for worker shutdown (unique objects)
INSERTION_SENTINEL = None
QUERY_SENTINEL = object()
