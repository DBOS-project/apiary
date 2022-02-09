load classes target/dynamic-apiary-0.1-SNAPSHOT.jar;

DROP PROCEDURE TruncateTables IF EXISTS;
CREATE PROCEDURE FROM CLASS org.dbos.apiary.procedures.TruncateTables;

DROP PROCEDURE IncrementProcedure IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE IncrementTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.IncrementProcedure;

DROP PROCEDURE AdditionFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE IncrementTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.AdditionFunction;

DROP PROCEDURE FibonacciFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE IncrementTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.FibonacciFunction;

DROP PROCEDURE IntSumFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE IncrementTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.IntSumFunction;