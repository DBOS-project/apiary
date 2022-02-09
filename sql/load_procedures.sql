load classes target/dynamic-apiary-0.1-SNAPSHOT.jar;

DROP PROCEDURE TruncateTables IF EXISTS;
CREATE PROCEDURE FROM CLASS org.dbos.apiary.procedures.TruncateTables;

DROP PROCEDURE AdditionFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.AdditionFunction;

DROP PROCEDURE FibonacciFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.FibonacciFunction;

DROP PROCEDURE FibSumFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.FibSumFunction;