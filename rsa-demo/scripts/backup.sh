#!/bin/bash

# Set the database name
DB_NAME="dbos"

# Set the PostgreSQL user and password
PG_USER="postgres"
PG_PASSWORD="dbos"

# Get the backup file name from the command line argument
if [ $# -eq 0 ]; then
  echo "Usage: $0 backup_file_name"
  exit 1
else
  BACKUP_FILE=$1
fi

# Use pg_dump to backup the database
pg_dump -Fc -U $PG_USER $DB_NAME -f $BACKUP_FILE

# Verify the backup was successful
if [ $? -eq 0 ]; then
  echo "Backup successful: $BACKUP_FILE"
else
  echo "Backup failed"
fi
