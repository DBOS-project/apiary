#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(readlink -f $0))
# Enter the root dir of the repo.
cd ${SCRIPT_DIR}/../

VOLTDB_BIN="${VOLT_HOME}/bin"

mvn -DskipTests package

${VOLTDB_BIN}/sqlcmd < sql/load_procedures.sql
