#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(readlink -f $0))
# Enter the root dir of the repo.
cd ${SCRIPT_DIR}/../

# Create obj and target directory
mkdir -p obj/
mkdir -p target/

VOLTDB_BIN="${VOLT_HOME}/bin"

mvn -DskipTests package

# TODO: maybe it's better to compile to separate jars for different components?
javac -cp "$VOLT_HOME/voltdb/*:$PWD/target/*" -d obj/sql $(find src/main/java/org/dbos/apiary/procedures/ -type f -name *.java)
jar cvf target/ApiaryProcedures.jar -C obj/sql .
${VOLTDB_BIN}/sqlcmd < sql/load_procedures.sql
