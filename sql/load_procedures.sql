load classes target/ApiaryProcedures.jar;

DROP PROCEDURE IncrementVSP IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE IncrementTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.IncrementVSP;
