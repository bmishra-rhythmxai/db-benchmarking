#!/bin/sh
# Replica entrypoint: wait for primary, pg_basebackup, then start Postgres.
set -e
export PGDATA="${PGDATA:-/var/lib/postgresql/data}"

if [ ! -f "$PGDATA/PG_VERSION" ]; then
  echo "Waiting for primary..."
  until PGPASSWORD="$POSTGRES_PASSWORD" pg_isready -h postgres-primary -p 5432 -U "$POSTGRES_USER" -d "$POSTGRES_DB"; do
    sleep 2
  done
  echo "Running pg_basebackup..."
  PGPASSWORD="$POSTGRES_PASSWORD" pg_basebackup -h postgres-primary -p 5432 -U "$POSTGRES_USER" -D "$PGDATA" -Fp -Xs -R
  chown -R postgres:postgres "$PGDATA"
fi

exec gosu postgres docker-entrypoint.sh postgres
