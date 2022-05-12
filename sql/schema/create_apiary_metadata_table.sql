CREATE TABLE ApiaryMetadata (
             PKEY INTEGER NOT NULL,
             MetadataKey VARCHAR(1024) NOT NULL,
             MetadataValue INTEGER,
             PRIMARY KEY (PKEY, MetadataKey)
);
PARTITION TABLE ApiaryMetadata ON COLUMN PKEY;