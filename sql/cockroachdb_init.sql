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
-- Range sizes in [65KiB, 100KiB].
ALTER TABLE kvtable CONFIGURE ZONE USING range_min_bytes=65536, range_max_bytes=102400;
-- Added here for ease of use. The below command is the default setting.
-- ALTER TABLE kvtable CONFIGURE ZONE USING range_min_bytes=134217728, range_max_bytes=536870912;
