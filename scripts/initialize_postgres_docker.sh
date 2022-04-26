#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(realpath $0))

# Start Postgres Docker image.
docker pull postgres

# Set the password to dbos, default user is postgres.
docker run -d --network host --rm --name="dbos-postgres" --env POSTGRES_PASSWORD=dbos postgres:latest