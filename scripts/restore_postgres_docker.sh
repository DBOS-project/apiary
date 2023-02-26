#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(realpath $0))

# Start Postgres Docker image.
docker pull postgres:14.5-bullseye

PGCONFIG=""  # Postgres config file.
NAME="tpcc-postgres"
PORT=5432

if [[ $# -eq 1 ]]; then
    PGCONFIG="$PWD/$1"
fi


# Set the password to dbos, default user is postgres.
if [[ -z "$PGCONFIG" ]]; then
    docker run -d --network host --rm --name="$NAME" --env PGDATA=/var/lib/postgresql-static/data --env POSTGRES_PASSWORD=dbos tpcc-postgres:latest
else
    # Use customized config file
    docker run -d --network host --rm --name="$NAME" --env PGDATA=/var/lib/postgresql-static/data --env POSTGRES_PASSWORD=dbos \
    -v "$PGCONFIG":/etc/postgresql/postgresql.conf \
    tpcc-postgres:latest -c 'config_file=/etc/postgresql/postgresql.conf'
fi


