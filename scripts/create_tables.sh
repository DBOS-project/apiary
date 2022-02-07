#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(readlink -f $0))
# Enter the root dir of the repo.
cd ${SCRIPT_DIR}/../

VOLTDB_BIN="${VOLT_HOME}/bin"

# Create tables.
SCHEMA_DIR="${SCRIPT_DIR}/../sql/schema"

for fname in ${SCHEMA_DIR}/create_*.sql; do
  ${VOLTDB_BIN}/sqlcmd < $fname
done
