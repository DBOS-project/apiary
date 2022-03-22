#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname $(realpath $0))

if [[ -z $(command -v cockroach) ]]; then
    echo "CockroachDB not installed!"
    # Install CockroachDB
    cd ${HOME}
    curl https://binaries.cockroachdb.com/cockroach-v21.2.5.linux-amd64.tgz | tar -xz
    sudo cp -i cockroach-v21.2.5.linux-amd64/cockroach /usr/local/bin/
    sudo mkdir -p /usr/local/lib/cockroach
    sudo cp -i cockroach-v21.2.5.linux-amd64/lib/libgeos.so /usr/local/lib/cockroach/
    sudo cp -i cockroach-v21.2.5.linux-amd64/lib/libgeos_c.so /usr/local/lib/cockroach/
fi

# Enter the root dir of the repo.
cd ${SCRIPT_DIR}/../

DB_PORTS=(26257 26258 26259)
JOIN_STR=""
for port in ${DB_PORTS[@]}
do
    JOIN_STR+=,localhost:$port
done
JOIN_STR=${JOIN_STR:1}

mkdir -p cockroach-data/
for i in "${!DB_PORTS[@]}"; do
    cockroach start \
        --insecure --store=cockroach-data/node$i\
        --listen-addr=localhost:${DB_PORTS[i]}\
        --http-addr=localhost:$((8080 + $i))\
        --join=$JOIN_STR\
        --background
done

cockroach init --insecure --host=localhost:26257

# Create DB and tables.
cat sql/cockroachdb_init.sql | cockroach sql --url "postgresql://root@localhost:26257?sslmode=disable"

echo "==== Finished initializing CockroachDB. ===="
