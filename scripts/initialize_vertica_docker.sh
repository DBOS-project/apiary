#!/bin/bash
set -x

SCRIPT_DIR=$(dirname $(realpath $0))

cd ${SCRIPT_DIR}/../

# Start Vertica Docker image.
docker pull vertica/vertica-ce

# Default no password, user is dbadmin.
docker run -d --rm -p 5433:5433 -p 5444:5444 \
           --mount type=volume,source=vertica-data,target=/data \
           --name vertica_ce \
           vertica/vertica-ce

# Wait until Vertica is ready.
MAX_TRY=30
cnt=0
while [[ -z "${ready}" ]]; do
  docker logs vertica_ce | grep -q "Vertica is now running" && ready="ready"
  cnt=$[$cnt+1]
  if [[ $cnt -eq ${MAX_TRY} ]]; then
    echo "Wait timed out. Vertica failed to start."
    exit 1
  fi
  sleep 5 # avoid busy loop
done

# Stop the default database VMart.
docker exec -it vertica_ce /opt/vertica/bin/adminTools -t stop_db -d VMart

# Start Apiary provenance database.
docker exec -it vertica_ce /opt/vertica/bin/adminTools -t create_db -s localhost -d apiary_provenance
docker exec -it vertica_ce /opt/vertica/bin/admintools -t start_db -d apiary_provenance

# Populate schema.
docker exec -i vertica_ce /opt/vertica/bin/vsql -t < sql/provenance_ddl.sql
