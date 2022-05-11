#!/bin/bash
set -ex

# Increase open files limit.
#ulimit -n 32768

SCRIPT_DIR=$(dirname $(readlink -f $0))
# Enter the root dir of the repo.
cd ${SCRIPT_DIR}/../

# Remove previous compilation files.
if [[ -d obj/ ]]; then
    rm -r obj/
fi

VOLTDB_BIN="${VOLT_HOME}/bin"

voltdb init -f --dir=/var/tmp --config ${SCRIPT_DIR}/local_config.xml
voltdb start -B --ignore=thp --dir=/var/tmp

# Wait until VoltDB is ready.
MAX_TRY=30
cnt=0
while [[ -z "${ready}" ]]; do
  python2 ${VOLTDB_BIN}/voltadmin status 2>&1 | grep -q "live host" && ready="ready"
  cnt=$[$cnt+1]
  if [[ $cnt -eq ${MAX_TRY} ]]; then
    echo "Wait timed out. VoltDB failed to start."
    exit 1
  fi
  sleep 5 # avoid busy loop
done

# Create tables
bash ${SCRIPT_DIR}/create_tables.sh

# Load stored procedures
bash ${SCRIPT_DIR}/update_voltdb_procedures.sh

echo "==== Finished initializing VoltDB. ===="
