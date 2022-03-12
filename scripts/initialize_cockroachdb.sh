#!/bin/bash
set -e

SCRIPT_DIR=$(dirname $(realpath $0))

if [[ -z $(command -v cockroach) ]]; then
    echo "CockroachDB not installed!"
    # Install CockroachDB
    cd ${HOME}
    curl https://binaries.cockroachdb.com/cockroach-v21.2.5.linux-amd64.tgz | tar -xz
    sudo cp -i cockroach-v21.2.5.linux-amd64/cockroach /usr/local/bin/
    sudo mkdir -p /usr/local/lib/cockroach
    sudo cp -i cockroach-v21.2.5.linux-amd64/lib/libgeos.so /usr/local/lib/cockroach/
    sudo cp -i cockroach-v21.2.5.linux-amd64/lib/libgeos_c.so /usr/local/lib/cockroach/

    # Only initialize once.
    # Enter the root dir of the repo.
    cd ${SCRIPT_DIR}/../

    cockroach init --insecure --host=localhost
fi

# Enter the root dir of the repo.
cd ${SCRIPT_DIR}/../

# Start DB.
cockroach start-single-node \
    --insecure \
    --listen-addr=localhost \
    --cache=.25 \
    --max-sql-memory=.25 \
    --background

echo "CREATE DATABASE IF NOT EXISTS test;" | cockroach sql --insecure

# TODO: Wait until CockroachDB is ready.
a='MAX_TRY=30
cnt=0
while [[ -z "${ready}" ]]; do
  cnt=$[$cnt+1]
  if [[ $cnt -eq ${MAX_TRY} ]]; then
    echo "Wait timed out. Failed to start."
    exit 1
  fi
  sleep 5 # avoid busy loop
done'

# TODO: Create tables

# TODO: Load stored procedures

echo "==== Finished initializing CockroachDB. ===="
