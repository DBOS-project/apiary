#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(realpath $0))

# Start Postgres Docker image.
docker pull postgres:14.5-bullseye

WAL_DIR="$PWD/archive" # WAL archive, this folder must exist and have proper ownership.

if [[ $# -eq 1 ]]; then
    WAL_DIR="$1"
fi

# Create the wal archive folder, and change the ownder to Postgres.
mkdir -p $WAL_DIR
sudo chown -R 999:999 $WAL_DIR

# Set the password to dbos, default user is postgres.
docker run --network host --rm --name="init-postgres" --env POSTGRES_PASSWORD=dbos \
    -v "$PWD/dbos-postgres.conf":/etc/postgresql/postgresql.conf \
    -v "$WAL_DIR":/tmp/postgres/archive \
    postgres:14.5-bullseye -c 'config_file=/etc/postgresql/postgresql.conf'

sleep 10

docker exec -i init-postgres psql -h localhost -U postgres -t < ${SCRIPT_DIR}/../init_postgres.sql
