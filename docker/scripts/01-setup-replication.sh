#!/bin/bash
set -e
# Allow POSTGRES_USER for normal connections and for replication (pg_basebackup)
cat >> "$PGDATA/pg_hba.conf" << EOF
host all ${POSTGRES_USER} 0.0.0.0/0 scram-sha-256
host all ${POSTGRES_USER} ::0/0 scram-sha-256
host replication ${POSTGRES_USER} 0.0.0.0/0 scram-sha-256
host replication ${POSTGRES_USER} ::0/0 scram-sha-256
EOF
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "ALTER USER \"$POSTGRES_USER\" WITH REPLICATION;"
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT pg_reload_conf();"
echo "Replication setup for $POSTGRES_USER OK"
