#!/bin/bash
set -ex

# Increase open files limit.
#ulimit -n 32768

SCRIPT_DIR=$(dirname $(readlink -f $0))
# Enter the root dir of the repo.
cd ${SCRIPT_DIR}/../

# Build docker image.
docker build -t voltdb .

# Start the docker.
docker run -d --network host --rm --name=apiary_voltdb voltdb:latest
