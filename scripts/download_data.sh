#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

cd ${SCRIPT_DIR}/../

mkdir -p data

cd data

wget https://kraftp-uniserve-data.s3.us-east-2.amazonaws.com/TPC-H-SF1/part.tbl
