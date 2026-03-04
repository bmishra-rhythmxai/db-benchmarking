#!/bin/bash
set -e
cat >> "$PGDATA/postgresql.conf" << 'EOF'
wal_level = replica
max_wal_senders = 4
listen_addresses = '*'
EOF
