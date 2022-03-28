CREATE TABLE KVTable (
                                PKEY INTEGER NOT NULL,
                                KVKey INTEGER NOT NULL,
                                KVValue INTEGER NOT NULL,
                                PRIMARY KEY (PKEY, KVKey)
);
PARTITION TABLE KVTable ON COLUMN PKEY;
CREATE INDEX KVTableIndex ON KVTable (KVKey);


CREATE TABLE MnistData (
                                    PKEY INTEGER NOT NULL,
                                    ID INTEGER NOT NULL,
                                    IMAGE VARCHAR(4000) NOT NULL,
                                    PRIMARY KEY (PKEY, ID)
);
PARTITION TABLE MnistData ON COLUMN PKEY;
CREATE INDEX MnistDataIndex ON MnistData (ID);

DROP TABLE MnistClassifications IF EXISTS;
CREATE TABLE MnistClassifications (
                                    PKEY INTEGER NOT NULL,
                                    ID INTEGER NOT NULL,
                                    CLASSIFICATION INTEGER NOT NULL,
                                    PRIMARY KEY (PKEY, ID)
);
PARTITION TABLE MnistClassifications ON COLUMN PKEY;
CREATE INDEX MnistClassificationsIndex ON MnistClassifications (ID);


CREATE TABLE ImagenetData (
                                    PKEY INTEGER NOT NULL,
                                    ID INTEGER NOT NULL,
                                    IMAGE VARBINARY(1048576) NOT NULL,
                                    PRIMARY KEY (PKEY, ID)
);
PARTITION TABLE ImagenetData ON COLUMN PKEY;
CREATE INDEX ImagenetDataIndex ON ImagenetData (ID);

DROP TABLE ImagenetClassifications IF EXISTS;
CREATE TABLE ImagenetClassifications (
                                    PKEY INTEGER NOT NULL,
                                    ID INTEGER NOT NULL,
                                    CLASSIFICATION INTEGER NOT NULL,
                                    PRIMARY KEY (PKEY, ID)
);
PARTITION TABLE ImagenetClassifications ON COLUMN PKEY;
CREATE INDEX ImagenetClassificationsIndex ON ImagenetClassifications (ID);
