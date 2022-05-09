CREATE TABLE RecordedOutputs (
                         PKEY INTEGER NOT NULL,
                         ExecID BIGINT NOT NULL,
                         FunctionID BIGINT NOT NULL,
                         StringOutput VARCHAR(1000),
                         IntOutput INTEGER,
                         StringArrayOutput VARBINARY(100000),
                         IntArrayOutput VARBINARY(100000),
                         FutureOutput BIGINT,
                         QueuedTasks VARBINARY(10000),
                         PRIMARY KEY (PKEY, ExecID, FunctionID)
);
PARTITION TABLE RecordedOutputs ON COLUMN PKEY;