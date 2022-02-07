load classes target/dynamic-apiary-0.1-SNAPSHOT.jar;

DROP PROCEDURE IncrementVSP IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE IncrementTable COLUMN PKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.IncrementVSP;

DROP PROCEDURE TruncateTables IF EXISTS;
CREATE PROCEDURE FROM CLASS dbos.procedures.TruncateTables;