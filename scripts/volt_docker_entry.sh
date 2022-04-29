#!/bin/bash
set -ex

# This script should be run inside a Docker container.
SCRIPT_DIR=$(dirname $(readlink -f $0))
cd ${SCRIPT_DIR}

./initialize_voltdb.sh

# Sleep forever
sleep 2000000000
