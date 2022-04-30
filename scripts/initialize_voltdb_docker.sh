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

# Wait until the docker is ready.
MAX_TRY=30
cnt=0
while [[ -z "${ready}" ]]; do
  cnt=$[$cnt+1]
  docker logs apiary_voltdb | grep -q "Finished initializing VoltDB." && ready="ready"
  if [[ $cnt -eq ${MAX_TRY} ]]; then
    echo "Wait timed out. VoltDB failed to start."
    exit 1
  fi
  sleep 10 # avoid busy loop
done


