#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(realpath $0))

# Start Postgres Docker image.
docker pull postgres

mkdir -p /tmp/postgresdemo/archive

# Set the password to dbos, default user is postgres.
docker run --network host --rm --name="apiary-postgres" --env POSTGRES_PASSWORD=dbos \
    -v "$PWD/dbos-postgres-2.conf":/etc/postgresql/postgresql.conf \
    -v "/tmp/postgresdemo/archive":/tmp/postgresdemo/archive \
    -e PGDATA=/var/lib/postgresql/data/pgdata \
    -v "/tmp/postgresdemo/clus2":/var/lib/postgresql/data \
    postgres:latest -c 'config_file=/etc/postgresql/postgresql.conf'
