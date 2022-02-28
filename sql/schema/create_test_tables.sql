CREATE TABLE KVTable (
                                KVKey INTEGER NOT NULL,
                                KVValue INTEGER NOT NULL,
                                PRIMARY KEY (KVKey)
);
PARTITION TABLE KVTable ON COLUMN KVKey;

CREATE TABLE PartitionInfo (
                               PartitionId INTEGER NOT NULL,
                               Pkey INTEGER DEFAULT -1,
                               HostId INTEGER NOT NULL, -- The host id is a logical id used by VoltDB to find a host.
                               Hostname VARCHAR(128) DEFAULT 'localhost',
                               IsLeader INTEGER DEFAULT 0,  -- IF 1, this host is the leader of that partition.
                               PRIMARY KEY (PartitionId, HostId) -- the partition id should be unique.
);
PARTITION TABLE PartitionInfo ON COLUMN PartitionId;
