#!/bin/bash
set -x

SCRIPT_DIR=$(dirname $(readlink -f $0))

cd ${SCRIPT_DIR}/../

LOG_DIR="/var/tmp/apiary_worker"
PIDFILE=${LOG_DIR}"/daemon.pid"

# Send SIGTERM to every process and delete PID file.
if [[ ! -f $PIDFILE ]]; then
    echo "No $PIDFILE found!"
    exit 1
fi

while read pid; do
  kill -9 $pid
done <$PIDFILE

rm $PIDFILE

echo "==== Apiary worker stopped. ===="
