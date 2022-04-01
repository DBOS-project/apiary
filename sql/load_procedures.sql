load classes target/DBOSProcedures.jar;

DROP PROCEDURE TruncateTables IF EXISTS;
CREATE PROCEDURE FROM CLASS org.dbos.apiary.procedures.voltdb.TruncateTables;

DROP PROCEDURE AdditionFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.AdditionFunction;

DROP PROCEDURE FibonacciFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.FibonacciFunction;

DROP PROCEDURE FibSumFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.FibSumFunction;

DROP PROCEDURE CounterFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.CounterFunction;

DROP PROCEDURE InsertFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.InsertFunction;

DROP PROCEDURE MnistInferenceFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE MnistData COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.MnistInferenceFunction;

DROP PROCEDURE MnistInsertFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE MnistClassifications COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.MnistInsertFunction;

DROP PROCEDURE MnistInsertDummyData IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE MnistData COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.MnistInsertDummyData;

DROP PROCEDURE ImagenetInferenceFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE ImagenetData COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.ImagenetInferenceFunction;

DROP PROCEDURE ImagenetInsertFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE ImagenetClassifications COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.ImagenetInsertFunction;

DROP PROCEDURE ImagenetInsertDummyData IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE ImagenetData COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.ImagenetInsertDummyData;
