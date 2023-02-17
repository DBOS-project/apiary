package org.dbos.apiary;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.moodle.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.junit.jupiter.api.*;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

// To test the bug and fixes of Moodle bugs. Moodle 59854, 28949, 43421.
public class MoodleTests {
    private static final Logger logger = LoggerFactory.getLogger(MoodleTests.class);

    private ApiaryWorker apiaryWorker;

    @BeforeAll
    public static void testConnection() {
        assumeTrue(TestUtils.testPostgresConnection());
        // ApiaryConfig.provenancePort = 5433;
        ApiaryConfig.isolationLevel = ApiaryConfig.REPEATABLE_READ;

        // Disable XDB transactions.
        ApiaryConfig.XDBTransactions = false;

        // Disable provenance tracking.
        ApiaryConfig.captureReads = false;
        ApiaryConfig.captureUpdates = false;
        ApiaryConfig.captureMetadata = false;

        // Record Input.
        ApiaryConfig.recordInput = true;
    }

    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos", TestUtils.provenanceDB, TestUtils.provenanceAddr);
            Connection provConn = conn.provConnection.get();
            PostgresConnection.dropTable(provConn, ApiaryConfig.tableFuncInvocations);
            PostgresConnection.dropTable(provConn, ApiaryConfig.tableRecordedInputs);
            PostgresConnection.dropTable(provConn, ProvenanceBuffer.PROV_QueryMetadata);
            conn.dropTable(ProvenanceBuffer.PROV_ApiaryMetadata);
            conn.dropTable(MDLUtil.MDL_FORUMSUBS_TABLE);
            conn.createTable(MDLUtil.MDL_FORUMSUBS_TABLE, MDLUtil.MDL_FORUM_SCHEMA);
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

    @AfterAll
    public static void resetFlags() {
        ApiaryConfig.recordInput = false;
    }

    @Test
    public void testForumSubscribeReplay() throws SQLException, InvalidProtocolBufferException, InterruptedException {
        logger.info("testForumSubscribeReplay");
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos", TestUtils.provenanceDB, TestUtils.provenanceAddr);

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, TestUtils.provenanceDB, TestUtils.provenanceAddr);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction(MDLUtil.FUNC_IS_SUBSCRIBED, ApiaryConfig.postgres, MDLIsSubscribed::new);
        apiaryWorker.registerFunction(MDLUtil.FUNC_FORUM_INSERT, ApiaryConfig.postgres, MDLForumInsert::new);
        apiaryWorker.registerFunction(MDLUtil.FUNC_FETCH_SUBSCRIBERS, ApiaryConfig.postgres, MDLFetchSubscribers::new);
        apiaryWorker.startServing();

        ProvenanceBuffer provBuff = apiaryWorker.workerContext.provBuff;
        assert(provBuff != null);

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction(MDLUtil.FUNC_IS_SUBSCRIBED, 123, 555).getInt();
        assertEquals(123, res);

        // Subscribe again, should return the same userId.
        res = client.executeFunction(MDLUtil.FUNC_IS_SUBSCRIBED, 123, 555).getInt();
        assertEquals(123, res);

        // Get a list of subscribers, should only contain one user entry.
        int[] resList = client.executeFunction(MDLUtil.FUNC_FETCH_SUBSCRIBERS,555).getIntArray();
        assertEquals(1, resList.length);
        assertEquals(123, resList[0]);

        // Check provenance and get executionID.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
        Connection provConn = provBuff.conn.get();
        Statement stmt = provConn.createStatement();

        String table = ApiaryConfig.tableFuncInvocations;
        ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY %s ASC;", table, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID));
        rs.next();
        long resExecId = rs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
        long resFuncId = rs.getLong(ProvenanceBuffer.PROV_FUNCID);
        String resFuncName = rs.getString(ProvenanceBuffer.PROV_PROCEDURENAME);
        assertTrue(resExecId >= 0);
        assertEquals(MDLUtil.FUNC_IS_SUBSCRIBED, resFuncName);

        // The second function should be a subscribe function.
        rs.next();
        long resExecId2 = rs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
        long resFuncId2 = rs.getLong(ProvenanceBuffer.PROV_FUNCID);
        assertEquals(resExecId, resExecId2);

        // The third function should be a new execution.
        rs.next();
        long resExecId3 = rs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
        long resFuncId3 = rs.getLong(ProvenanceBuffer.PROV_FUNCID);
        assertNotEquals(resExecId, resExecId3);
        assertEquals(resFuncId, resFuncId3);

        // The fourth function should be a new fetchSubscribers.
        rs.next();
        long resExecId4 = rs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
        long resFuncId4 = rs.getLong(ProvenanceBuffer.PROV_FUNCID);
        assertNotEquals(resExecId, resExecId4);
        assertNotEquals(resExecId3, resExecId4);
        assertEquals(resFuncId, resFuncId4);

        // Retroactively execute all.
        // Reset the database and re-execute, stop before the last execution.
        conn.truncateTable(MDLUtil.MDL_FORUMSUBS_TABLE, false);
        res = client.retroReplay(resExecId, resExecId4, ApiaryConfig.ReplayMode.ALL.getValue()).getInt();
        assertEquals(123, res);

        // Retro replay again, but this time replay the entire trace.
        conn.truncateTable(MDLUtil.MDL_FORUMSUBS_TABLE, false);
        resList = client.retroReplay(resExecId, Long.MAX_VALUE, ApiaryConfig.ReplayMode.ALL.getValue()).getIntArray();
        assertEquals(1, resList.length);
        assertEquals(123, resList[0]);

        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
    }

    @Test
    public void testForumSubscribeRetro() throws SQLException, InterruptedException, InvalidProtocolBufferException, ExecutionException {
        logger.info("testForumSubscribeRetro");

        // Run concurrent test until we find duplications. Then retroactively replay everything.
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos", TestUtils.provenanceDB, TestUtils.provenanceAddr);

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, TestUtils.provenanceDB, TestUtils.provenanceAddr);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction(MDLUtil.FUNC_IS_SUBSCRIBED, ApiaryConfig.postgres, MDLIsSubscribed::new);
        apiaryWorker.registerFunction(MDLUtil.FUNC_FORUM_INSERT, ApiaryConfig.postgres, MDLForumInsert::new);
        apiaryWorker.registerFunction(MDLUtil.FUNC_FETCH_SUBSCRIBERS, ApiaryConfig.postgres, MDLFetchSubscribers::new);
        apiaryWorker.startServing();

        ProvenanceBuffer provBuff = apiaryWorker.workerContext.provBuff;
        assert(provBuff != null);

        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient("localhost"));

        // Start a thread pool.
        ExecutorService threadPool = Executors.newFixedThreadPool(2);

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
                    res = client.get().executeFunction(MDLUtil.FUNC_IS_SUBSCRIBED, userId, forumId).getInt();
                } catch (Exception e) {
                    res = -1;
                }
                return res;
            }
        }

        // Try many times until we find duplications.
        int maxTry = 1000;
        int[] resList = null;
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
            resList = client.get().executeFunction(MDLUtil.FUNC_FETCH_SUBSCRIBERS, i+maxTry).getIntArray();
            if (resList.length > 1) {
                logger.info("Found duplications! User: {}, Forum: {}", i, i+maxTry);
                break;
            }
        }

        // Only continue the test if we have found duplications.
        if (resList == null || resList.length == 1) {
            logger.warn("Did not find duplicates. Skip test");
        }
        assumeTrue(resList.length > 1);
        threadPool.shutdown();

        // Wait for provenance to be exported.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        // Check the original execution.
        Connection provConn = provBuff.conn.get();
        Statement stmt = provConn.createStatement();
        String provQuery = String.format("SELECT * FROM %s ORDER BY %s ASC;", ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        ResultSet rs = stmt.executeQuery(provQuery);
        rs.next();
        long resExecId = rs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
        long resFuncId = rs.getLong(ProvenanceBuffer.PROV_FUNCID);
        String resFuncName = rs.getString(ProvenanceBuffer.PROV_PROCEDURENAME);
        assertTrue(resExecId >= 0);
        assumeTrue(resFuncId == 0);
        assertEquals(MDLUtil.FUNC_IS_SUBSCRIBED, resFuncName);

        // Reset the table and replay all.
        conn.truncateTable(MDLUtil.MDL_FORUMSUBS_TABLE, false);
        int[] retroResList = client.get().retroReplay(resExecId, Long.MAX_VALUE, ApiaryConfig.ReplayMode.ALL.getValue()).getIntArray();
        assertEquals(resList.length, retroResList.length);
        assertTrue(Arrays.equals(resList, retroResList));
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        // Now, register the new code and see if it can get the correct result.
        apiaryWorker.shutdown(); // Stop the existing worker.
        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, TestUtils.provenanceDB, TestUtils.provenanceAddr);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction(MDLUtil.FUNC_IS_SUBSCRIBED, ApiaryConfig.postgres, MDLSubscribeTxn::new, true);  // Register the new one.
        // The old one.
        apiaryWorker.registerFunction(MDLUtil.FUNC_FORUM_INSERT, ApiaryConfig.postgres, MDLForumInsert::new, false);
        apiaryWorker.registerFunction(MDLUtil.FUNC_FETCH_SUBSCRIBERS, ApiaryConfig.postgres, MDLFetchSubscribers::new, false);

        // No need to register function set, because we have a single function now.
        // apiaryWorker.registerFunctionSet("MDLIsSubscribed", "MDLIsSubscribed", "MDLForumInsert");
        apiaryWorker.startServing();

        provBuff = apiaryWorker.workerContext.provBuff;
        assert(provBuff != null);

        conn.truncateTable(MDLUtil.MDL_FORUMSUBS_TABLE, false);
        int[] retroList = client.get().retroReplay(resExecId, Long.MAX_VALUE, ApiaryConfig.ReplayMode.ALL.getValue()).getIntArray();
        // TODO: Repeatable read allows write skew. Does not check serializability.
        if (ApiaryConfig.isolationLevel == ApiaryConfig.REPEATABLE_READ) {
            assertTrue(retroList.length >= 1);
        } else {
            assertEquals(1, retroList.length);
        }
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        // Retro replay again, but now we enable selective replay.
        conn.truncateTable(MDLUtil.MDL_FORUMSUBS_TABLE, false);
        int retroRes = client.get().retroReplay(resExecId, Long.MAX_VALUE, ApiaryConfig.ReplayMode.SELECTIVE.getValue()).getInt();
        assertTrue(retroRes != -1);
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
    }
}
