#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(realpath $0))

# Start Postgres Docker image.
docker pull postgres:14.5-bullseye

# Set the password to dbos, default user is postgres.
docker run -d --network host --rm --name="apiary-postgres" --env POSTGRES_PASSWORD=dbos postgres:14.5-bullseye
