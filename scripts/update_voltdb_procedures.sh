#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(readlink -f $0))
# Enter the root dir of the repo.

VOLTDB_BIN="${VOLT_HOME}/bin"

cd ${SCRIPT_DIR}/../

# Test if Vertica JDBC is downloaded.
if [[ ! -f target/vertica/vertica-jdbc-10.1.1-0.jar ]]; then
    mkdir -p target/vertica
    cd target/vertica
    curl -LJO https://storage.googleapis.com/apiary_public/vertica-jdbc-10.1.1-0.jar
    jar xf vertica-jdbc-10.1.1-0.jar
fi

cd ${SCRIPT_DIR}/../
mvn -DskipTests package

javac -cp "$VOLT_HOME/voltdb/*:$PWD/target/*:$PWD/target/vertica/*" -d obj/sql $(find src/main/java/org/dbos/apiary/procedures/voltdb -type f -name *.java)
jar cvf target/DBOSProcedures.jar -C obj/sql . -C target/classes org/dbos/apiary/function -C target/classes org/dbos/apiary/connection -C target/classes org/dbos/apiary/voltdb -C target/classes org/dbos/apiary/utilities  -C  target/vertica .

${VOLTDB_BIN}/sqlcmd < sql/load_procedures.sql
