package org.dbos.apiary;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.wordpress.*;
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
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

// To test the bug and fixes of WordPress-11073: https://core.trac.wordpress.org/ticket/11073
public class WordPressTests {
    private static final Logger logger = LoggerFactory.getLogger(WordPressTests.class);

    private ApiaryWorker apiaryWorker;

    @BeforeAll
    public static void testConnection() {
        assumeTrue(TestUtils.testPostgresConnection());
        ApiaryConfig.provenancePort = TestUtils.provenancePort;
        ApiaryConfig.isolationLevel = ApiaryConfig.REPEATABLE_READ;

        // Disable XDB transactions.
        ApiaryConfig.XDBTransactions = false;

        // Disable provenance tracking.
        ApiaryConfig.captureReads = false;
        ApiaryConfig.captureUpdates = false;
        ApiaryConfig.captureMetadata = false;

        // Record input.
        ApiaryConfig.recordInput = true;
    }

    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos",
                    TestUtils.provenanceDB, TestUtils.provenanceAddr);
            Connection provConn = conn.provConnection.get();
            PostgresConnection.dropTable(provConn, ApiaryConfig.tableFuncInvocations);
            PostgresConnection.dropTable(provConn, ApiaryConfig.tableRecordedInputs);
            PostgresConnection.dropTable(provConn, ProvenanceBuffer.PROV_QueryMetadata);
            conn.dropTable(ProvenanceBuffer.PROV_ApiaryMetadata);
            conn.dropTable(WPUtil.WP_POSTS_TABLE);
            conn.createTable(WPUtil.WP_POSTS_TABLE, WPUtil.WP_POSTS_SCHEMA);
            conn.dropTable(WPUtil.WP_POSTMETA_TABLE);
            conn.createTable(WPUtil.WP_POSTMETA_TABLE, WPUtil.WP_POSTMETA_SCHEMA);
            conn.dropTable(WPUtil.WP_COMMENTS_TABLE);
            conn.createTable(WPUtil.WP_COMMENTS_TABLE, WPUtil.WP_COMMENTS_SCHEMA);
            conn.dropTable(WPUtil.WP_OPTIONS_TABLE);
            conn.createTable(WPUtil.WP_OPTIONS_TABLE, WPUtil.WP_OPTIONS_SCHEMA);
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
    public void testPostSerialized() throws SQLException, InvalidProtocolBufferException, InterruptedException {
        logger.info("testPostSerialized");
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos", TestUtils.provenanceDB, TestUtils.provenanceAddr);

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, TestUtils.provenanceDB, TestUtils.provenanceAddr);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("WPAddPost", ApiaryConfig.postgres, WPAddPost::new);
        apiaryWorker.registerFunction("WPAddComment", ApiaryConfig.postgres, WPAddComment::new);
        apiaryWorker.registerFunction("WPGetPostComments", ApiaryConfig.postgres, WPGetPostComments::new);
        apiaryWorker.registerFunction("WPTrashPost", ApiaryConfig.postgres, WPTrashPost::new);
        apiaryWorker.registerFunction("WPTrashComments", ApiaryConfig.postgres, WPTrashComments::new);
        apiaryWorker.registerFunction("WPUntrashPost", ApiaryConfig.postgres, WPUntrashPost::new);
        apiaryWorker.registerFunction("WPCheckCommentStatus", ApiaryConfig.postgres, WPCheckCommentStatus::new);
        apiaryWorker.startServing();
        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("WPAddComment", 123, 3450, "this should not work.").getInt();
        assertEquals(-1, res);
        res = client.executeFunction("WPAddPost", 123, "test post").getInt();
        assertEquals(0, res);
        res = client.executeFunction("WPAddComment", 123, 3450, "test comment to a post.").getInt();
        assertEquals(0, res);
        res = client.executeFunction("WPAddComment", 123, 3460, "second test comment to a post.").getInt();
        assertEquals(0, res);

        String[] resList = client.executeFunction("WPGetPostComments", 123).getStringArray();
        assertEquals(3, resList.length);
        assertTrue(resList[0].equals("test post"));
        assertTrue(resList[1].equals("test comment to a post."));
        assertTrue(resList[2].equals("second test comment to a post."));

        // Trash the post.
        res = client.executeFunction("WPTrashPost", 123).getInt();
        assertEquals(123, res);

        // Check status. Should all be post-trashed.
        resList = client.executeFunction("WPCheckCommentStatus", 123).getStringArray();
        assertEquals(1, resList.length);
        assertTrue(resList[0].equals(WPUtil.WP_STATUS_POST_TRASHED));

        // Untrash the post.
        res = client.executeFunction("WPUntrashPost", 123).getInt();
        assertEquals(0, res);

        // Check status again. Should all be visible.
        resList = client.executeFunction("WPCheckCommentStatus", 123).getStringArray();
        assertEquals(1, resList.length);
        assertTrue(resList[0].equals(WPUtil.WP_STATUS_VISIBLE));

        // Check provenance.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
    }

    @Test
    public void testPostConcurrentRetro() throws SQLException, InvalidProtocolBufferException, InterruptedException {
        // Try to reproduce the bug where the new comment comes between post trashed and comment trashed. So the new comment would be marked as trashed but cannot be restored afterwards.
        logger.info("testWPConcurrentRetro");
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos", TestUtils.provenanceDB, TestUtils.provenanceAddr);

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, TestUtils.provenanceDB, TestUtils.provenanceAddr);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("WPAddPost", ApiaryConfig.postgres, WPAddPost::new);
        apiaryWorker.registerFunction("WPAddComment", ApiaryConfig.postgres, WPAddComment::new);
        apiaryWorker.registerFunction("WPGetPostComments", ApiaryConfig.postgres, WPGetPostComments::new);
        apiaryWorker.registerFunction("WPTrashPost", ApiaryConfig.postgres, WPTrashPost::new);
        apiaryWorker.registerFunction("WPTrashComments", ApiaryConfig.postgres, WPTrashComments::new);
        apiaryWorker.registerFunction("WPUntrashPost", ApiaryConfig.postgres, WPUntrashPost::new);
        apiaryWorker.registerFunction("WPCheckCommentStatus", ApiaryConfig.postgres, WPCheckCommentStatus::new);
        apiaryWorker.startServing();

        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient("localhost"));

        // Start a thread pool.
        ExecutorService threadPool = Executors.newFixedThreadPool(2);

        class WpTask implements Callable<Integer> {
            private final int postId;
            private final int commentId;
            private final String action;

            public WpTask(int postId, int commentId, String action) {
                this.postId = postId;
                this.commentId = commentId;
                this.action = action;
            }

            @Override
            public Integer call() {
                int res;
                if (action.equals("trashpost")) {
                    try {
                        // Trash a post.
                        res = client.get().executeFunction("WPTrashPost", postId).getInt();
                    } catch (Exception e) {
                        res = -1;
                    }
                } else {
                    try {
                        // Add a comment.
                        res = client.get().executeFunction("WPAddComment", postId, commentId, action).getInt();
                    } catch (Exception e) {
                        res = -1;
                    }
                }
                return res;
            }

        }

        // Try many times until we find inconsistency.
        int postIds = 0;
        int commentIds = 0;
        int maxTry = 200;
        int intRes;
        String[] strAryRes;
        boolean foundInconsistency = false;
        for (int i = 0; i < maxTry; i++) {
            // Add a new post and a comment.
            intRes = client.get().executeFunction("WPAddPost", postIds, "test post " + postIds).getInt();
            assertEquals(0, intRes);
            intRes = client.get().executeFunction("WPAddComment", postIds, commentIds, "test comment " + commentIds).getInt();
            commentIds++;
            assertEquals(0, intRes);

            // Launch concurrent tasks.
            Future<Integer> trashResFut = threadPool.submit(new WpTask(postIds, -1, "trashpost"));
            // Add arbitrary delay.
            Thread.sleep(ThreadLocalRandom.current().nextInt(1, 5));
            Future<Integer> commentResFut = threadPool.submit(new WpTask(postIds, commentIds, "test comment concurrent " + commentIds));

            int trashRes, commentRes;
            try {
                trashRes = trashResFut.get();
                commentRes = commentResFut.get();
                assertEquals(postIds, trashRes);
                assertEquals(0, commentRes);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }

            // Restore the post.
            intRes = client.get().executeFunction("WPUntrashPost", postIds).getInt();
            assertEquals(0, intRes);

            String[] resList = client.get().executeFunction("WPGetPostComments", postIds).getStringArray();
            assertTrue(resList.length > 1);

            // Check results. Try to find inconsistency.
            strAryRes = client.get().executeFunction("WPCheckCommentStatus", postIds).getStringArray();
            if (strAryRes.length > 1) {
                logger.info("Found inconsistency!");
                foundInconsistency = true;
                break;
            }
            postIds++;
            commentIds++;
        }
        assertTrue(foundInconsistency);
        threadPool.shutdown();

        // Check provenance.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
        ProvenanceBuffer provBuff = apiaryWorker.workerContext.provBuff;
        assert(provBuff != null);
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
        assertEquals("WPAddPost", resFuncName);

        // Reset the table and replay all.
        conn.truncateTable(WPUtil.WP_POSTS_TABLE, false);
        conn.truncateTable(WPUtil.WP_COMMENTS_TABLE, false);
        conn.truncateTable(WPUtil.WP_POSTMETA_TABLE, false);

        strAryRes = client.get().retroReplay(resExecId, Long.MAX_VALUE, ApiaryConfig.ReplayMode.ALL.getValue()).getStringArray();
        assertTrue(strAryRes.length > 1);
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        // Register the new code and see if we can get the correct result.
        apiaryWorker.shutdown();
        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, TestUtils.provenanceDB, TestUtils.provenanceAddr);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("WPAddPost", ApiaryConfig.postgres, WPAddPost::new, false);
        // Use the new code.
        apiaryWorker.registerFunction("WPAddComment", ApiaryConfig.postgres, WPAddCommentFixed::new, true);
        apiaryWorker.registerFunction("WPGetPostComments", ApiaryConfig.postgres, WPGetPostComments::new, false);
        apiaryWorker.registerFunction("WPTrashPost", ApiaryConfig.postgres, WPTrashPost::new, false);
        apiaryWorker.registerFunction("WPTrashComments", ApiaryConfig.postgres, WPTrashComments::new, false);
        apiaryWorker.registerFunction("WPUntrashPost", ApiaryConfig.postgres, WPUntrashPost::new, false);
        apiaryWorker.registerFunction("WPCheckCommentStatus", ApiaryConfig.postgres, WPCheckCommentStatus::new, false);
        apiaryWorker.registerFunctionSet(WPUtil.FUNC_TRASHPOST, WPUtil.FUNC_TRASHPOST, WPUtil.FUNC_TRASHCOMMENTS);
        apiaryWorker.startServing();

        provBuff = apiaryWorker.workerContext.provBuff;
        assert(provBuff != null);

        conn.truncateTable(WPUtil.WP_POSTS_TABLE, false);
        conn.truncateTable(WPUtil.WP_COMMENTS_TABLE, false);
        conn.truncateTable(WPUtil.WP_POSTMETA_TABLE, false);

        strAryRes = client.get().retroReplay(resExecId, Long.MAX_VALUE, ApiaryConfig.ReplayMode.ALL.getValue()).getStringArray();
        // Check provenance.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        // TODO: repeatable read cannot actually fix the bug.
        if (ApiaryConfig.isolationLevel == ApiaryConfig.REPEATABLE_READ) {
            assertTrue(strAryRes.length >= 1);
        } else {
            assertEquals(1, strAryRes.length);
        }

        // Retro replay again, but use selective replay.
        conn.truncateTable(WPUtil.WP_POSTS_TABLE, false);
        conn.truncateTable(WPUtil.WP_COMMENTS_TABLE, false);
        conn.truncateTable(WPUtil.WP_POSTMETA_TABLE, false);

        intRes = client.get().retroReplay(resExecId, Long.MAX_VALUE, ApiaryConfig.ReplayMode.SELECTIVE.getValue()).getInt();
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
        assertEquals(0, intRes); // Should successfully untrash the last post.
    }

    @Test
    public void testOptionSerialized() throws SQLException, InvalidProtocolBufferException, InterruptedException {
        logger.info("testOptionSerialized");
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos", TestUtils.provenanceDB, TestUtils.provenanceAddr);

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, TestUtils.provenanceDB, TestUtils.provenanceAddr);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("WPGetOption", ApiaryConfig.postgres, WPGetOption::new);
        apiaryWorker.registerFunction("WPOptionExists", ApiaryConfig.postgres, WPOptionExists::new);
        apiaryWorker.registerFunction("WPInsertOption", ApiaryConfig.postgres, WPInsertOption::new);
        apiaryWorker.registerFunction(WPUtil.FUNC_UPDATEOPTION, ApiaryConfig.postgres, WPInsertOptionFixed::new, false);
        apiaryWorker.startServing();
        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("WPOptionExists", "option1", "value1", "no").getInt();
        assertEquals(0, res); // return 0 as we newly inserted the option.

        // Add again, should return 0 because we update it.
        res = client.executeFunction("WPOptionExists", "option1", "value2", "no").getInt();
        assertEquals(0, res);

        // Get option value.
        String resStr = client.executeFunction("WPGetOption", "option1").getString();
        assertEquals("value2", resStr);

        // Check provenance.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
    }

    @Test
    public void testOptionConcurrentRetro() throws SQLException, InvalidProtocolBufferException, InterruptedException {
        logger.info("testOptionConcurrentRetro");

        // Run concurrent requests until we find an error. Then retroactively replay.
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos", TestUtils.provenanceDB, TestUtils.provenanceAddr);

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, TestUtils.provenanceDB, TestUtils.provenanceAddr);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("WPGetOption", ApiaryConfig.postgres, WPGetOption::new, false);
        apiaryWorker.registerFunction("WPOptionExists", ApiaryConfig.postgres, WPOptionExists::new, false);
        apiaryWorker.registerFunction("WPInsertOption", ApiaryConfig.postgres, WPInsertOption::new, false);
        apiaryWorker.registerFunction(WPUtil.FUNC_UPDATEOPTION, ApiaryConfig.postgres, WPInsertOptionFixed::new, false);
        apiaryWorker.startServing();

        ProvenanceBuffer provBuff = apiaryWorker.workerContext.provBuff;
        assert(provBuff != null);

        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient("localhost"));

        // Start a thread pool.
        ExecutorService threadPool = Executors.newFixedThreadPool(2);

        class OpsTask implements Callable<Integer> {
            private final String opName;
            private final String opValue;
            private final String opAutoLoad;

            public OpsTask(String opName, String opValue, String opAutoLoad) {
                this.opName = opName;
                this.opValue = opValue;
                this.opAutoLoad = opAutoLoad;
            }

            @Override
            public Integer call() {
                int res;
                try {
                    FunctionOutput fo = client.get().executeFunction("WPOptionExists", opName, opValue, opAutoLoad);
                    if ((fo.errorMsg != null) && !fo.errorMsg.isEmpty()) {
                        logger.info("Function error message: {}", fo.errorMsg);
                    }
                    res = fo.getInt();
                } catch (Exception e) {
                    res = -2;
                }
                return res;
            }
        }

        // Try many times until we find duplications.
        int maxTry = 1000;
        int res = -2;
        int i;
        for (i = 0; i < maxTry; i++) {
            // Launch concurrent tasks.
            Future<Integer> res1Fut = threadPool.submit(new OpsTask("option-" + i, "value0-" + i, "no"));
            // Add arbitrary delay.
            Thread.sleep(ThreadLocalRandom.current().nextInt(2));
            Future<Integer> res2Fut = threadPool.submit(new OpsTask("option-" + i, "value1-" + i, "no"));

            int res1, res2;
            try {
                res1 = res1Fut.get();
                res2 = res2Fut.get();
                assertNotEquals(-2, res1);
                assertNotEquals(-2, res2);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }

            // Check the option, should be the first one.
            String resStr = client.get().executeFunction("WPGetOption", "option-" + i).getString();
            assertTrue(resStr.contains("value"));
            if (res2 == -1) {
                logger.info("Found error! Option: {}", i);
                break;
            }
        }
        threadPool.shutdown();
        assumeTrue(i < maxTry);

        // Wait for provenance to be exported.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
        Connection provConn = provBuff.conn.get();
        Statement stmt = provConn.createStatement();
        String provQuery = String.format("SELECT * FROM %s ORDER BY %s ASC;", ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        ResultSet rs = stmt.executeQuery(provQuery);
        rs.next();
        long resExecId = rs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
        long resFuncId = rs.getLong(ProvenanceBuffer.PROV_FUNCID);
        String resFuncName = rs.getString(ProvenanceBuffer.PROV_PROCEDURENAME);
        assertTrue(resExecId >= 0);
        assertTrue(resFuncId == 0);
        assertEquals("WPOptionExists", resFuncName);

        // Reset the table and replay all.
        conn.truncateTable(WPUtil.WP_OPTIONS_TABLE, false);

        // Replay everything and check we get the same result.
        String resStr = client.get().retroReplay(resExecId, Long.MAX_VALUE, ApiaryConfig.ReplayMode.ALL.getValue()).getString();

        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
        assertTrue(resStr.equals("value0-" + i));

        // Register the new function and retro replay all.
        conn.truncateTable(WPUtil.WP_OPTIONS_TABLE, false);
        apiaryWorker.registerFunction(WPUtil.FUNC_INSERTOPTION, ApiaryConfig.postgres, WPInsertOptionFixed::new, true);
        apiaryWorker.registerFunctionSet(WPUtil.FUNC_OPTIONEXISTS, WPUtil.FUNC_OPTIONEXISTS, WPUtil.FUNC_INSERTOPTION);

        resStr = client.get().retroReplay(resExecId, Long.MAX_VALUE, ApiaryConfig.ReplayMode.ALL.getValue()).getString();
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
        // The fixed one should let the failed one commit.
        // TODO: we might not be able to guarantee order for retroaction. Find a better way to check?
        assertTrue(resStr.contains("value"));

        // Selective replay. The last one should be value1-i succeeded, so should return 0.
        conn.truncateTable(WPUtil.WP_OPTIONS_TABLE, false);
        res = client.get().retroReplay(resExecId, Long.MAX_VALUE, ApiaryConfig.ReplayMode.SELECTIVE.getValue()).getInt();
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
        assertEquals(0, res);
    }
}
