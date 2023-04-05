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

# Use pg_restore to restore the database from the backup file
pg_restore -U $PG_USER --clean -d $DB_NAME $BACKUP_FILE

# Verify the restore was successful
if [ $? -eq 0 ]; then
  echo "Restore successful"
else
  echo "Restore failed"
fi
