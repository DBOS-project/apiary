#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(realpath $0))

# Start Postgres Docker image.
docker pull postgres:14.5-bullseye

PGCONFIG=""  # Postgres config file.
if [[ $# -eq 1 ]]; then
    PGCONFIG="$PWD/$1"
fi

# Set the password to dbos, default user is postgres.
if [[ -z "$PGCONFIG" ]]; then
    docker run -d --network host --rm --name="apiary-postgres" --env POSTGRES_PASSWORD=dbos postgres:14.5-bullseye
else
    # Use customized config file
    docker run -d --network host --rm --name="apiary-postgres" --env POSTGRES_PASSWORD=dbos \
    -v "$PGCONFIG":/etc/postgresql/postgresql.conf \
    postgres:14.5-bullseye -c 'config_file=/etc/postgresql/postgresql.conf'
fi

sleep 10

docker exec -i apiary-postgres psql -h localhost -U postgres -t < ${SCRIPT_DIR}/init_postgres.sql
