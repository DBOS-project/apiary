load classes target/DBOSProcedures.jar;

DROP PROCEDURE TruncateTables IF EXISTS;
CREATE PROCEDURE FROM CLASS org.dbos.apiary.procedures.voltdb.TruncateTables;

DROP PROCEDURE AdditionFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.tests.AdditionFunction;

DROP PROCEDURE FibonacciFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.tests.FibonacciFunction;

DROP PROCEDURE FibSumFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.tests.FibSumFunction;

DROP PROCEDURE CounterFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.tests.CounterFunction;

DROP PROCEDURE InsertFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.tests.InsertFunction;

DROP PROCEDURE SynchronousCounter IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.tests.SynchronousCounter;