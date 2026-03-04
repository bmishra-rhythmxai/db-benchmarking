#!/bin/bash
set -e
cat >> "$PGDATA/postgresql.conf" << 'EOF'
synchronous_commit = off
shared_buffers = 256MB
wal_buffers = 16MB
wal_writer_flush_after = 128
wal_compression = off
wal_keep_size = 1GB
max_wal_size = 2GB
checkpoint_flush_after = 256
checkpoint_timeout = 15min
io_workers = 4
autovacuum_max_workers = 2
effective_io_concurrency = 64
maintenance_io_concurrency = 32
max_parallel_maintenance_workers = 4
bgwriter_flush_after = 0
EOF
