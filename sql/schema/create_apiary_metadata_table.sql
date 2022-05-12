CREATE TABLE ApiaryMetadata (
             MetadataKey VARCHAR(1024) NOT NULL,
             MetadataValue INTEGER,
             PRIMARY KEY (MetadataKey)
);
PARTITION TABLE ApiaryMetadata ON COLUMN MetadataKey;