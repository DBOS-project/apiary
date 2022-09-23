package org.dbos.apiary;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.replay.PostgresFetchSubscribers;
import org.dbos.apiary.procedures.postgres.replay.PostgresForumSubscribe;
import org.dbos.apiary.procedures.postgres.replay.PostgresIsSubscribed;
import org.dbos.apiary.procedures.postgres.retwis.*;
import org.dbos.apiary.procedures.postgres.tests.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class PostgresTests {
    private static final Logger logger = LoggerFactory.getLogger(PostgresTests.class);

    private ApiaryWorker apiaryWorker;

    @BeforeAll
    public static void testConnection() {
        assumeTrue(TestUtils.testPostgresConnection());
        // Set the isolation level to serializable.
        ApiaryConfig.isolationLevel = ApiaryConfig.SERIALIZABLE;

        // Disable XDB transactions.
        ApiaryConfig.XDBTransactions = false;
    }

    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
            conn.dropTable("FuncInvocations");
            conn.dropTable("KVTable");
            conn.createTable("KVTable", "KVKey integer PRIMARY KEY NOT NULL, KVValue integer NOT NULL");
            conn.dropTable("KVTableTwo");
            conn.createTable("KVTableTwo", "KVKeyTwo integer PRIMARY KEY NOT NULL, KVValueTwo integer NOT NULL");
            conn.dropTable("RetwisPosts");
            conn.createTable("RetwisPosts", "UserID integer NOT NULL, PostID integer NOT NULL, Timestamp integer NOT NULL, Post varchar(1000) NOT NULL");
            conn.dropTable("RetwisFollowees");
            conn.createTable("RetwisFollowees", "UserID integer NOT NULL, FolloweeID integer NOT NULL");
            conn.dropTable("ForumSubscription");
            conn.createTable("ForumSubscription", "UserId integer NOT NULL, ForumId integer NOT NULL");
            conn.dropTable(ProvenanceBuffer.PROV_ApiaryMetadata);
            conn.dropTable(ProvenanceBuffer.PROV_QueryMetadata);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to Postgres.");
            assumeTrue(false);
        }
        apiaryWorker = null;
    }

    @AfterEach
    public void cleanupWorker() {
        if (apiaryWorker != null) {
            apiaryWorker.shutdown();
        }
    }

    @Test
    public void testForumSubscribe() throws SQLException, InvalidProtocolBufferException, InterruptedException {
        logger.info("testForumSubscribe");
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("PostgresIsSubscribed", ApiaryConfig.postgres, PostgresIsSubscribed::new);
        apiaryWorker.registerFunction("PostgresForumSubscribe", ApiaryConfig.postgres, PostgresForumSubscribe::new);
        apiaryWorker.registerFunction("PostgresFetchSubscribers", ApiaryConfig.postgres, PostgresFetchSubscribers::new);
        apiaryWorker.startServing();

        ProvenanceBuffer provBuff = apiaryWorker.workerContext.provBuff;
        assert(provBuff != null);
        Connection provConn = provBuff.conn.get();
        Statement stmt = provConn.createStatement();
        String[] tables = {ProvenanceBuffer.PROV_FuncInvocations, "forumsubscriptionevents"};
        for (String table : tables) {
            stmt.execute(String.format("TRUNCATE TABLE %s;", table));
        }

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresIsSubscribed", 123, 555).getInt();
        assertEquals(123, res);

        // Subscribe again, should return the same userId.
        res = client.executeFunction("PostgresIsSubscribed", 123, 555).getInt();
        assertEquals(123, res);

        // Get a list of subscribers, should only contain one user entry.
        int[] resList = client.executeFunction("PostgresFetchSubscribers",555).getIntArray();
        assertEquals(1, resList.length);
        assertEquals(123, resList[0]);

        // Check provenance and get executionID.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        String table = ProvenanceBuffer.PROV_FuncInvocations;
        ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY %s ASC;", table, ProvenanceBuffer.PROV_APIARY_TIMESTAMP));
        rs.next();
        long txid1 = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        long resExecId = rs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
        String resFuncName = rs.getString(ProvenanceBuffer.PROV_PROCEDURENAME);
        assertTrue(resExecId >= 0);
        assertEquals(PostgresIsSubscribed.class.getName(), resFuncName);

        // Replay the execution of the first one.
        // TODO: add more replay features.
        res = client.replayFunction(resExecId,"PostgresIsSubscribed", 123, 555).getInt();
        assertEquals(123, res);

        // Check provenance.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
    }

    @Test
    public void testForumSubscribeConcurrent() throws SQLException, InvalidProtocolBufferException, InterruptedException, ExecutionException {
        // Run until duplications happen.
        logger.info("testForumSubscribeConcurrent");
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("PostgresIsSubscribed", ApiaryConfig.postgres, PostgresIsSubscribed::new);
        apiaryWorker.registerFunction("PostgresForumSubscribe", ApiaryConfig.postgres, PostgresForumSubscribe::new);
        apiaryWorker.registerFunction("PostgresFetchSubscribers", ApiaryConfig.postgres, PostgresFetchSubscribers::new);
        apiaryWorker.startServing();

        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient("localhost"));

        // Start a thread pool.
        ExecutorService threadPool = Executors.newFixedThreadPool(4);

        class SubsTask implements Callable<Integer> {
            private final int userId;
            private final int forumId;

            public SubsTask(int userId, int forumId) {
                this.userId = userId;
                this.forumId = forumId;
            }

            @Override
            public Integer call() {
                int res;
                try {
                    res = client.get().executeFunction("PostgresIsSubscribed", userId, forumId).getInt();
                } catch (Exception e) {
                    res = -1;
                }
                return res;
            }
        }

        // Try many times until we find duplications.
        int maxTry = 1000;
        for (int i = 0; i < maxTry; i++) {
            // Push two concurrent tasks.
            List<SubsTask> tasks = new ArrayList<>();
            tasks.add(new SubsTask(i, i+maxTry));
            tasks.add(new SubsTask(i, i+maxTry));
            List<Future<Integer>> futures = threadPool.invokeAll(tasks);
            for (Future<Integer> future : futures) {
                if (!future.isCancelled()) {
                    int res = future.get();
                    assertTrue(res != -1);
                }
            }
            // Check subscriptions.
            int[] resList = client.get().executeFunction("PostgresFetchSubscribers", i+maxTry).getIntArray();
            if (resList.length > 1) {
                logger.info("Found duplications! User: {}, Forum: {}", i, i+maxTry);
                break;
            }
        }

        threadPool.shutdown();
        // Check provenance.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
    }

    @Test
    public void testFibPostgres() throws InvalidProtocolBufferException, SQLException {
        logger.info("testFibPostgres");

        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("PostgresFibonacciFunction", ApiaryConfig.postgres, PostgresFibonacciFunction::new);
        apiaryWorker.registerFunction("PostgresFibSumFunction", ApiaryConfig.postgres, PostgresFibSumFunction::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresFibonacciFunction", 1).getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresFibonacciFunction", 6).getInt();
        assertEquals(8, res);

        res = client.executeFunction("PostgresFibonacciFunction", 10).getInt();
        assertEquals(55, res);
    }

    @Test
    public void testRetwisPostgres() throws InvalidProtocolBufferException, SQLException {
        logger.info("testRetwisPostgres");

        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("RetwisPost", ApiaryConfig.postgres, RetwisPost::new);
        apiaryWorker.registerFunction("RetwisFollow", ApiaryConfig.postgres, RetwisFollow::new);
        apiaryWorker.registerFunction("RetwisGetPosts", ApiaryConfig.postgres, RetwisGetPosts::new);
        apiaryWorker.registerFunction("RetwisGetFollowees", ApiaryConfig.postgres, RetwisGetFollowees::new);
        apiaryWorker.registerFunction("RetwisGetTimeline", ApiaryConfig.postgres, RetwisGetTimeline::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int resInt;
        resInt = client.executeFunction("RetwisPost", 0, 0, 0, "hello0").getInt();
        assertEquals(0, resInt);
        resInt = client.executeFunction("RetwisPost", 0, 1, 1, "hello1").getInt();
        assertEquals(0, resInt);
        resInt = client.executeFunction("RetwisPost", 1, 2, 0, "hello2").getInt();
        assertEquals(1, resInt);
        resInt = client.executeFunction("RetwisFollow", 1, 0).getInt();
        assertEquals(1, resInt);
        resInt = client.executeFunction("RetwisFollow", 1, 1).getInt();
        assertEquals(1, resInt);

        String[] postResult = client.executeFunction("RetwisGetPosts", 0).getStringArray();
        assertArrayEquals(new String[]{"hello0", "hello1"}, postResult);

        int[] followees = client.executeFunction("RetwisGetFollowees", 1).getIntArray();
        assertEquals(2, followees.length);
        assertTrue(followees[0] == 0 && followees[1] == 1 || followees[0] == 1 && followees[1] == 0);

        String[] timeline = client.executeFunction("RetwisGetTimeline", 1).getStringArray();
        assertTrue(Arrays.asList(timeline).contains("hello0"));
        assertTrue(Arrays.asList(timeline).contains("hello1"));
        assertTrue(Arrays.asList(timeline).contains("hello2"));
    }

    @Test
    public void testPostgresProvenance() throws InvalidProtocolBufferException, SQLException, InterruptedException {
        logger.info("testPostgresProvenance");

        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("PostgresProvenanceBasic", ApiaryConfig.postgres, PostgresProvenanceBasic::new);
        apiaryWorker.startServing();

        ProvenanceBuffer provBuff = apiaryWorker.workerContext.provBuff;
        assert(provBuff != null);

        // Wait a bit so previous provenance capture data would be flushed out.
        Thread.sleep(ProvenanceBuffer.exportInterval * 4);
        Connection provConn = provBuff.conn.get();
        Statement stmt = provConn.createStatement();
        String[] tables = {"FUNCINVOCATIONS", "KVTableEvents"};
        for (String table : tables) {
            stmt.execute(String.format("TRUNCATE TABLE %s;", table));
        }

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        int key = 10, value = 100;
        res = client.executeFunction("PostgresProvenanceBasic", key, value).getInt();
        assertEquals(101, res);

        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        // Check provenance tables.
        // Check function invocation table.
        String table = ProvenanceBuffer.PROV_FuncInvocations;
        ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY %s DESC;", table, ProvenanceBuffer.PROV_APIARY_TIMESTAMP));
        rs.next();
        long txid1 = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        long resExecId = rs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
        String resService = rs.getString(ProvenanceBuffer.PROV_SERVICE);
        String resFuncName = rs.getString(ProvenanceBuffer.PROV_PROCEDURENAME);
        assertEquals("DefaultService", resService);
        assertEquals(PostgresProvenanceBasic.class.getName(), resFuncName);

        rs.next();
        long txid2 = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        resExecId = rs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
        resService = rs.getString(ProvenanceBuffer.PROV_SERVICE);
        resFuncName = rs.getString(ProvenanceBuffer.PROV_PROCEDURENAME);
        assertEquals("DefaultService", resService);
        assertEquals(PostgresProvenanceBasic.class.getName(), resFuncName);

        // Inner transaction should have the same transaction ID.
        assertEquals(txid1, txid2);

        // Check KVTable.
        table = "KVTableEvents";
        int expectedSeqNum = 0;
        rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY %s;", table, ProvenanceBuffer.PROV_APIARY_TIMESTAMP));
        rs.next();

        // Should be a lookup for key=1. But with NULL data.
        long resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        int resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        int resKey = rs.getInt("KVKey");
        assertTrue(rs.wasNull());
        int resValue = rs.getInt("KVValue");
        assertTrue(rs.wasNull());
        int resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(expectedSeqNum, resSeqNum);
        expectedSeqNum += 1;
        assertEquals(txid2, resTxid);
        assertEquals(ProvenanceBuffer.ExportOperation.READ.getValue(), resExportOp);

        rs.next();
        // Should be an insert for key=1.
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(expectedSeqNum, resSeqNum);
        expectedSeqNum += 1;
        assertEquals(txid2, resTxid);
        assertEquals(ProvenanceBuffer.ExportOperation.INSERT.getValue(), resExportOp);
        assertEquals(1, resKey);
        assertEquals(value, resValue);

        // Should be an insert for the key value.
        rs.next();
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(expectedSeqNum, resSeqNum);
        expectedSeqNum += 1;
        assertEquals(ProvenanceBuffer.ExportOperation.INSERT.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(value, resValue);

        // Should be a read.
        rs.next();
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(expectedSeqNum, resSeqNum);
        expectedSeqNum += 1;
        assertEquals(ProvenanceBuffer.ExportOperation.READ.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(100, resValue);

        // Should be an update.
        rs.next();
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(expectedSeqNum, resSeqNum);
        expectedSeqNum += 1;
        assertEquals(ProvenanceBuffer.ExportOperation.UPDATE.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(value+1, resValue);

        // Should be a read again.
        rs.next();
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(expectedSeqNum, resSeqNum);
        expectedSeqNum += 1;
        assertEquals(ProvenanceBuffer.ExportOperation.READ.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(101, resValue);

        // Should be a delete.
        rs.next();
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(expectedSeqNum, resSeqNum);
        assertEquals(ProvenanceBuffer.ExportOperation.DELETE.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(value+1, resValue);
    }

    @Test
    public void testPostgresProvenanceJoins() throws InvalidProtocolBufferException, SQLException, InterruptedException {
        logger.info("testPostgresProvenanceJoins");

        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("PostgresProvenanceJoins", ApiaryConfig.postgres, PostgresProvenanceJoins::new);
        apiaryWorker.startServing();

        ProvenanceBuffer provBuff = apiaryWorker.workerContext.provBuff;
        assert(provBuff != null);

        Thread.sleep(ProvenanceBuffer.exportInterval * 4);
        Connection provConn = provBuff.conn.get();
        Statement stmt = provConn.createStatement();
        String[] tables = {"FUNCINVOCATIONS", "KVTableEvents", "KVTableTwoEvents"};
        for (String table : tables) {
            stmt.execute(String.format("TRUNCATE TABLE %s;", table));
        }

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresProvenanceJoins", 1, 2, 3).getInt();
        assertEquals(5, res);

        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        // Check KVTable.
        String table = "KVTableEvents";
        ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY %s;", table, ProvenanceBuffer.PROV_APIARY_TIMESTAMP));
        rs.next();

        // Should be an insert for key=1.
        int resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        int resKey = rs.getInt("KVKey");
        int resValue = rs.getInt("KVValue");
        assertEquals(ProvenanceBuffer.ExportOperation.INSERT.getValue(), resExportOp);
        assertEquals(1, resKey);
        assertEquals(2, resValue);

        // Should be a read.
        rs.next();
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        assertEquals(ProvenanceBuffer.ExportOperation.READ.getValue(), resExportOp);
        assertEquals(2, resValue);

        // Check KVTable.
        table = "KVTableTwoEvents";
        rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY %s;", table, ProvenanceBuffer.PROV_APIARY_TIMESTAMP));
        rs.next();

        // Should be an insert for key=1.
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKeyTwo");
        resValue = rs.getInt("KVValueTwo");
        assertEquals(ProvenanceBuffer.ExportOperation.INSERT.getValue(), resExportOp);
        assertEquals(1, resKey);
        assertEquals(3, resValue);

        // Should be a read.
        rs.next();
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKeyTwo");
        resValue = rs.getInt("KVValueTwo");
        assertEquals(ProvenanceBuffer.ExportOperation.READ.getValue(), resExportOp);
        assertEquals(3, resValue);
    }

    @Test
    public void testPostgresProvenanceMultiRows() throws InvalidProtocolBufferException, SQLException, InterruptedException {
        logger.info("testPostgresProvenanceMultiRows");

        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("PostgresProvenanceMultiRows", ApiaryConfig.postgres, PostgresProvenanceMultiRows::new);
        apiaryWorker.startServing();

        ProvenanceBuffer provBuff = apiaryWorker.workerContext.provBuff;
        assert(provBuff != null);

        // Wait a bit so previous provenance capture data would be flushed out.
        Thread.sleep(ProvenanceBuffer.exportInterval * 4);
        Connection provConn = provBuff.conn.get();
        Statement stmt = provConn.createStatement();
        String[] tables = {"FUNCINVOCATIONS", "KVTableEvents"};
        for (String table : tables) {
            stmt.execute(String.format("TRUNCATE TABLE %s;", table));
        }

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        int key1 = 10, value1 = 100;
        int key2 = 20, value2 = 11;
        res = client.executeFunction("PostgresProvenanceMultiRows", key1, value1, key2, value2).getInt();
        assertEquals(111, res);

        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        // Check provenance tables.
        // Check function invocation table.
        String table = "FUNCINVOCATIONS";
        ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY %s DESC;", table, ProvenanceBuffer.PROV_APIARY_TIMESTAMP));
        rs.next();
        long txid1 = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        long resExecId = rs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
        String resService = rs.getString(ProvenanceBuffer.PROV_SERVICE);
        String resFuncName = rs.getString(ProvenanceBuffer.PROV_PROCEDURENAME);
        assertEquals("DefaultService", resService);
        assertEquals(PostgresProvenanceMultiRows.class.getName(), resFuncName);

        // Check KVTable.
        table = "KVTableEvents";
        rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY %s, KVKEY;", table, ProvenanceBuffer.PROV_APIARY_TIMESTAMP));
        rs.next();

        // Should be an insert for key1.
        long resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        int resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        int resKey = rs.getInt("KVKey");
        int resValue = rs.getInt("KVValue");
        assertEquals(txid1, resTxid);
        assertEquals(ProvenanceBuffer.ExportOperation.INSERT.getValue(), resExportOp);
        assertEquals(key1, resKey);
        assertEquals(value1, resValue);

        // Should be an insert for the key2.
        rs.next();
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        assertEquals(ProvenanceBuffer.ExportOperation.INSERT.getValue(), resExportOp);
        assertEquals(key2, resKey);
        assertEquals(value2, resValue);

        // Should be a read for key1.
        rs.next();
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        assertEquals(ProvenanceBuffer.ExportOperation.READ.getValue(), resExportOp);
        assertEquals(key1, resKey);
        assertEquals(value1, resValue);

        // Should be a read again for key2.
        rs.next();
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        assertEquals(ProvenanceBuffer.ExportOperation.READ.getValue(), resExportOp);
        assertEquals(key2, resKey);
        assertEquals(value2, resValue);
    }
}
