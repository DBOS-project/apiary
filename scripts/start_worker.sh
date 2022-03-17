#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

SCHEDULER=""
if [[ $# -eq 1 ]]; then
    SCHEDULER="$1"
elif [[ $# -gt 1 ]]; then
    echo "Too many arguments!"
    exit 1
fi

if [[ -z ${SCHEDULER} ]]; then
    SCHEDULER="naive"
fi

cd ${SCRIPT_DIR}/../

LOG_DIR="/var/tmp/apiary_worker"

mkdir -p ${LOG_DIR}

# Compile the executable.
mvn -DskipTests package

LOGFILE=${LOG_DIR}"/worker.log"
PIDFILE=${LOG_DIR}"/daemon.pid"

java -verbose:gc -XX:+UnlockExperimentalVMOptions -XX:+UseShenandoahGC -Xms4G -Xmx4G -jar target/apiary-worker-exec-fat-exec.jar -s ${SCHEDULER} >${LOGFILE} 2>&1 &
PID=$!
echo $PID >>${PIDFILE}

echo "===== Apiary worker started ====="
