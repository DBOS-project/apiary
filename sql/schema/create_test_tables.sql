CREATE TABLE IncrementTable (
                                PKEY INTEGER NOT NULL,
                                IncrementKey INTEGER NOT NULL,
                                IncrementValue INTEGER NOT NULL,
                                PRIMARY KEY (PKEY, IncrementKey)
);
PARTITION TABLE IncrementTable ON COLUMN PKEY;
CREATE INDEX IncrementTableIndex ON IncrementTable (IncrementKey);