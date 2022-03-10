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

DROP PROCEDURE InferenceFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE MnistData COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.InferenceFunction;

DROP PROCEDURE InsertMnistFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE MnistClassifications COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.InsertMnistFunction;

DROP PROCEDURE InsertDummyMnistData IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE MnistData COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.InsertDummyMnistData;
