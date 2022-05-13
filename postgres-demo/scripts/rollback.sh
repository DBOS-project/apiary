#!/bin/bash
set -e

SCRIPT_DIR=$(dirname $(readlink -f $0))
# Enter the root dir of the repo.
cd ${SCRIPT_DIR}/../

if [[ ! $# -eq 1 ]]; then
  echo "Usage: $0 <timestamp>"
  exit 1
fi

java -jar target/demo-rollback-exec-fat-exec.jar -timestamp $1