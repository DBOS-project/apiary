package org.dbos.apiary;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.wordpress.*;
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
        // Set the isolation level to serializable.
        ApiaryConfig.isolationLevel = ApiaryConfig.SERIALIZABLE;

        // Disable XDB transactions.
        ApiaryConfig.XDBTransactions = false;

        // Disable read tracking.
        ApiaryConfig.captureReads = false;
    }

    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos");
            conn.dropTable(ApiaryConfig.tableFuncInvocations);
            conn.dropTable(ApiaryConfig.tableRecordedInputs);
            conn.dropTable(ProvenanceBuffer.PROV_ApiaryMetadata);
            conn.dropTable(ProvenanceBuffer.PROV_QueryMetadata);
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

    @Test
    public void testPostSerialized() throws SQLException, InvalidProtocolBufferException, InterruptedException {
        logger.info("testPostSerialized");
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
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
        ApiaryConfig.recordInput = true;
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
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
            Thread.sleep(ThreadLocalRandom.current().nextInt(5));
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
        String provQuery = String.format("SELECT * FROM %s ORDER BY %s ASC;", ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_EXECUTIONID);
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
        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("WPAddPost", ApiaryConfig.postgres, WPAddPost::new);
        // Use the new code.
        apiaryWorker.registerFunction("WPAddComment", ApiaryConfig.postgres, WPAddCommentFixed::new, true);
        apiaryWorker.registerFunction("WPGetPostComments", ApiaryConfig.postgres, WPGetPostComments::new);
        apiaryWorker.registerFunction("WPTrashPost", ApiaryConfig.postgres, WPTrashPost::new);
        apiaryWorker.registerFunction("WPTrashComments", ApiaryConfig.postgres, WPTrashComments::new);
        apiaryWorker.registerFunction("WPUntrashPost", ApiaryConfig.postgres, WPUntrashPost::new);
        apiaryWorker.registerFunction("WPCheckCommentStatus", ApiaryConfig.postgres, WPCheckCommentStatus::new);
        apiaryWorker.startServing();

        provBuff = apiaryWorker.workerContext.provBuff;
        assert(provBuff != null);

        conn.truncateTable(WPUtil.WP_POSTS_TABLE, false);
        conn.truncateTable(WPUtil.WP_COMMENTS_TABLE, false);
        conn.truncateTable(WPUtil.WP_POSTMETA_TABLE, false);

        strAryRes = client.get().retroReplay(resExecId, Long.MAX_VALUE, ApiaryConfig.ReplayMode.ALL.getValue()).getStringArray();
        assertEquals(1, strAryRes.length);

        ApiaryConfig.recordInput = false; // Reset flags.

        // Check provenance.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        // Retro replay again, but use selective replay.
        conn.truncateTable(WPUtil.WP_POSTS_TABLE, false);
        conn.truncateTable(WPUtil.WP_COMMENTS_TABLE, false);
        conn.truncateTable(WPUtil.WP_POSTMETA_TABLE, false);

        intRes = client.get().retroReplay(resExecId, Long.MAX_VALUE, ApiaryConfig.ReplayMode.SELECTIVE.getValue()).getInt();
        assertEquals(0, intRes); // Should successfully untrashed the last post.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
    }

    @Test
    public void testOptionSerialized() throws SQLException, InvalidProtocolBufferException, InterruptedException {
        logger.info("testOptionSerialized");
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("WPGetOption", ApiaryConfig.postgres, WPGetOption::new);
        apiaryWorker.registerFunction("WPOptionExists", ApiaryConfig.postgres, WPOptionExists::new);
        apiaryWorker.registerFunction("WPInsertOption", ApiaryConfig.postgres, WPInsertOption::new);
        apiaryWorker.startServing();
        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("WPOptionExists", "option1", "value1", "no").getInt();
        assertEquals(0, res); // return 0 as we newly inserted the option.

        // Add again, should return 1 because the option already exists.
        res = client.executeFunction("WPOptionExists", "option1", "value2", "no").getInt();
        assertEquals(1, res);

        // Get option value.
        String resStr = client.executeFunction("WPGetOption", "option1").getString();
        assertEquals("value1", resStr);

        // Check provenance.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
    }

    @Test
    public void testOptionConcurrent() throws SQLException, InvalidProtocolBufferException, InterruptedException {
        logger.info("testOptionConcurrent");

        // Run concurrent requests until we find an error. Then retroactively replay.
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("WPGetOption", ApiaryConfig.postgres, WPGetOption::new);
        apiaryWorker.registerFunction("WPOptionExists", ApiaryConfig.postgres, WPOptionExists::new);
        apiaryWorker.registerFunction("WPInsertOption", ApiaryConfig.postgres, WPInsertOption::new);
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
                    res = client.get().executeFunction("WPOptionExists", opName, opValue, opAutoLoad).getInt();
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
    }
}
