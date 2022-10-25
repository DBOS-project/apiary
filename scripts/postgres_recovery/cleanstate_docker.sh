#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(realpath $0))

# Start Postgres Docker image, use the Debian version.
# docker pull postgres:14.5-bullseye
docker pull qianl15/apiary-postgres-image:latest

# Set the password to dbos, default user is postgres.
docker run -it --network host --rm --name="apiary-postgres" --env POSTGRES_PASSWORD=dbos --entrypoint /bin/bash apiary-postgres-image:latest
