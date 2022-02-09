CREATE TABLE KVTable (
                                PKEY INTEGER NOT NULL,
                                KVKey INTEGER NOT NULL,
                                KVValue INTEGER NOT NULL,
                                PRIMARY KEY (PKEY, KVKey)
);
PARTITION TABLE KVTable ON COLUMN PKEY;
CREATE INDEX KVTableIndex ON KVTable (KVKey);