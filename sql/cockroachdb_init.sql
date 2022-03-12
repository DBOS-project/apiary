SET sql_safe_updates = FALSE;

DROP DATABASE IF EXISTS test CASCADE;
CREATE DATABASE IF NOT EXISTS test;

USE test;

CREATE TABLE KVTable (
    KVKey integer PRIMARY KEY NOT NULL, 
    KVValue integer NOT NULL
);
-- Disable replication.
ALTER TABLE KVTable CONFIGURE ZONE USING num_replicas=1;
