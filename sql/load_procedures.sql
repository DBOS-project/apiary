load classes target/DBOSProcedures.jar;

DROP PROCEDURE TruncateTables IF EXISTS;
CREATE PROCEDURE FROM CLASS org.dbos.apiary.procedures.voltdb.TruncateTables;

DROP PROCEDURE GetApiaryClientID IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE ApiaryMetadata COLUMN PKEY PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.GetApiaryClientID;

DROP PROCEDURE AdditionFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN KVKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.tests.AdditionFunction;

DROP PROCEDURE FibonacciFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN KVKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.tests.FibonacciFunction;

DROP PROCEDURE FibSumFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN KVKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.tests.FibSumFunction;

DROP PROCEDURE CounterFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN KVKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.tests.CounterFunction;

DROP PROCEDURE InsertFunction IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN KVKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.tests.InsertFunction;

DROP PROCEDURE SynchronousCounter IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN KVKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.tests.SynchronousCounter;

DROP PROCEDURE RetwisPost IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE RetwisPosts COLUMN UserID PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.retwis.RetwisPost;

DROP PROCEDURE RetwisGetPosts IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE RetwisPosts COLUMN UserID PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.retwis.RetwisGetPosts;

DROP PROCEDURE RetwisFollow IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE RetwisFollowees COLUMN UserID PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.retwis.RetwisFollow;

DROP PROCEDURE RetwisGetTimeline IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE RetwisFollowees COLUMN UserID PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.retwis.RetwisGetTimeline;

DROP PROCEDURE RetwisGetFollowees IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE RetwisFollowees COLUMN UserID PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.retwis.RetwisGetFollowees;

DROP PROCEDURE IncrementProcedure IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN KVKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.increment.IncrementProcedure;

DROP PROCEDURE VoltProvenanceBasic IF EXISTS;
CREATE PROCEDURE PARTITION ON TABLE KVTable COLUMN KVKey PARAMETER 0 FROM CLASS org.dbos.apiary.procedures.voltdb.tests.VoltProvenanceBasic;