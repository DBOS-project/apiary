#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(realpath $0))

# Start Postgres Docker image.
docker pull postgres:14.5-bullseye

BASE_DIR="$PWD/base"  # Base backup. Need to have the proper postgresql.conf and recovery.signal
WAL_DIR="$PWD/pg_wal" # WAL archive

if [[ $# -eq 2 ]]; then
    BASE_DIR="$1" # Input from cmd line.
    WAL_DIR="$2"
fi

# Set the password to dbos, default user is postgres.
docker run --network host --rm --name="recovery-postgres" --env POSTGRES_PASSWORD=dbos \
    -v "$BASE_DIR":/tmp/dbosbase \
    -v "$WAL_DIR":/tmp/postgres/archive \
    -e PGDATA=/tmp/dbosbase \
    postgres:14.5-bullseye -c 'config_file=/tmp/dbosbase/postgresql.conf'
