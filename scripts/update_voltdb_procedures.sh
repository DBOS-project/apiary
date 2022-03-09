#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(readlink -f $0))
# Enter the root dir of the repo.
cd ${SCRIPT_DIR}/../

VOLT_HOME="/voltdb-ent-9.3.2"
VOLTDB_BIN="${VOLT_HOME}/bin"

mvn -DskipTests package

javac -cp "$VOLT_HOME/voltdb/*:$PWD/target/*" -d obj/sql $(find src/main/java/org/dbos/apiary/procedures/voltdb -type f -name *.java)
jar cvf target/DBOSProcedures.jar -C obj/sql . -C target/classes org/dbos/apiary/interposition -C target/classes org/dbos/apiary/voltdb -C target/classes org/dbos/apiary/executor -C target/classes org/dbos/apiary/utilities
${VOLTDB_BIN}/sqlcmd < sql/load_procedures.sql
