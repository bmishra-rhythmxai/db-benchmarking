#!/bin/bash
# Create Citus extension when using a Citus image. No-op on vanilla Postgres (script does not fail).
psql -v ON_ERROR_STOP=0 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE EXTENSION IF NOT EXISTS citus;" || true
