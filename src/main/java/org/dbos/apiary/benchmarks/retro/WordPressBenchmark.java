package org.dbos.apiary.benchmarks.retro;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.benchmarks.RetroBenchmark;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.wordpress.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class WordPressBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(WordPressBenchmark.class);

    private static final int threadPoolSize = 4;
    private static final int numWorker = 4;
    private static final int numOptions = 100;
    private static final int numPosts = 1000;
    private static final int initCommentsPerPost = 1;
    private static final AtomicInteger commentId = new AtomicInteger(initCommentsPerPost);

    private static final int threadWarmupMs = 5000;  // First 5 seconds of request would be warm-up requests.

    private static final Collection<Long> readTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> writeTimes = new ConcurrentLinkedQueue<>();

    public enum WPOpType {
        ADD_POST(0),
        ADD_COMMENT(1),
        TRASH_POST(2),
        GET_COMMENTS(3),
        GET_OPTION(4),
        UPDATE_OPTION(5);

        private int value;

        WPOpType (int value) {
            this.value = value;
        }

        public int getValue() {
            return this.value;
        }
    }

    static class WpTask implements Callable<Integer> {
        private final WPOpType wpOpType; // Which type of operaton?
        private final BlockingQueue<ApiaryWorkerClient> clientPool; // A pool of clients.

        // For requests on the Options table.
        private final String optName;
        private final String optValue;
        private final String optAutoLoad;

        // For requests on the posts/comments table.
        private final Integer postId;
        private final Integer commentId;
        private final String content;
        private final Collection<Long> execTimes;

        public WpTask(BlockingQueue<ApiaryWorkerClient> clientPool, WPOpType type, Collection<Long> execTimes, String optName, String optValue, String optAutoLoad) {
            this.clientPool = clientPool;
            this.wpOpType = type;
            this.optName = optName;
            this.optValue = optValue;
            this.optAutoLoad = optAutoLoad;
            this.postId = null;
            this.commentId = null;
            this.content = null;
            this.execTimes = execTimes;
        }

        public WpTask(BlockingQueue<ApiaryWorkerClient> clientPool, WPOpType type, Collection<Long> execTimes, int postId, int commentId, String content) {
            this.clientPool = clientPool;
            this.wpOpType = type;
            this.optName = null;
            this.optValue = null;
            this.optAutoLoad = null;
            this.postId = postId;
            this.commentId = commentId;
            this.content = content;
            this.execTimes = execTimes;
        }

        @Override
        public Integer call() {
            ApiaryWorkerClient client = null;
            try {
                client = clientPool.poll(10, TimeUnit.SECONDS);
                assert client != null;
            } catch (InterruptedException e) {
                logger.error("Failed to get a client. Return...");
                return -3;
            }

            int res;
            long t0 = System.nanoTime();
            try {
                if (wpOpType.equals(WPOpType.ADD_POST)) {
                    res = client.executeFunction("WPAddPost", postId, content).getInt();
                } else if (wpOpType.equals(WPOpType.ADD_COMMENT)) {
                    res = client.executeFunction("WPAddComment", postId, commentId, content).getInt();
                } else if (wpOpType.equals(WPOpType.TRASH_POST)) {
                    res = client.executeFunction("WPTrashPost", postId).getInt();
                } else if (wpOpType.equals(WPOpType.GET_COMMENTS)) {
                    String[] resList = client.executeFunction("WPGetPostComments", postId).getStringArray();
                    assert (resList.length >= 1);
                    res = resList.length;
                } else if (wpOpType.equals(WPOpType.GET_OPTION)) {
                    String resStr = client.executeFunction("WPGetOption", optName).getString();
                    assert (!resStr.isEmpty());
                    res = resStr.length();
                } else if (wpOpType.equals(WPOpType.UPDATE_OPTION)) {
                    res = client.executeFunction("WPOptionExists", optName, optValue, optAutoLoad).getInt();
                } else {
                    logger.error("Unrecognized option type {}", wpOpType.value);
                    clientPool.add(client);
                    return -2;
                }
            } catch(InvalidProtocolBufferException e){
                logger.error("Failed to execute request, type {}", wpOpType.name());
                clientPool.add(client);
                return -3;
            }
            if (execTimes != null) {
                execTimes.add(System.nanoTime() - t0);
            }
            clientPool.add(client);
            return res;
        }
    }

    public static void benchmark(String dbAddr, Integer interval, Integer duration, boolean skipLoad, int retroMode, long startExecId, long endExecId, String bugFix, List<Integer> percentages) throws SQLException, InvalidProtocolBufferException, InterruptedException {
        ApiaryConfig.isolationLevel = ApiaryConfig.SERIALIZABLE;
        int addCommentPC = percentages.get(0);
        int trashPostPC = percentages.get(1);
        int untrashPostPC = percentages.get(2);
        int getCommentsPC = percentages.get(3);
        int updateOptionPC = percentages.get(4);
        logger.info("Percentages: addComment {}, trashPost {}, untrashPost {}, getComments {}, updateOption {}, getOption{}", addCommentPC, trashPostPC, untrashPostPC, getCommentsPC, updateOptionPC);

        boolean hasProv = (ApiaryConfig.captureReads || ApiaryConfig.captureUpdates) ? true : false;  // Enable provenance?

        if (retroMode == ApiaryConfig.ReplayMode.NOT_REPLAY.getValue()) {
            if (!skipLoad) {
                // Only reset tables if we do initial runs.
                resetAllTables(dbAddr);
            }
        } else {
            ApiaryConfig.recordInput = false;
            if (!skipLoad){
                // TODO: for now, we just drop entire data tables. We can probably use point-in-time recovery, or recover through our selective replay.
                resetAppTables(dbAddr);
            }
        }

        PostgresConnection pgConn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "dbos");

        ApiaryWorker apiaryWorker;
        if (hasProv) {
            // Enable provenance logging in the worker.
            apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numWorker, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        } else {
            // Disable provenance.
            apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numWorker);
        }
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pgConn);

        // Register all functions.
        apiaryWorker.registerFunction(WPUtil.FUNC_ADDPOST, ApiaryConfig.postgres, WPAddPost::new);
        apiaryWorker.registerFunction(WPUtil.FUNC_ADDCOMMENT, ApiaryConfig.postgres, WPAddComment::new);
        apiaryWorker.registerFunction(WPUtil.FUNC_GETPOSTCOMMENTS, ApiaryConfig.postgres, WPGetPostComments::new);
        apiaryWorker.registerFunction(WPUtil.FUNC_TRASHPOST, ApiaryConfig.postgres, WPTrashPost::new);
        apiaryWorker.registerFunction(WPUtil.FUNC_TRASHCOMMENTS, ApiaryConfig.postgres, WPTrashComments::new);
        apiaryWorker.registerFunction(WPUtil.FUNC_UNTRASHPOST, ApiaryConfig.postgres, WPUntrashPost::new);
        apiaryWorker.registerFunction(WPUtil.FUNC_COMMENTSTATUS, ApiaryConfig.postgres, WPCheckCommentStatus::new);
        apiaryWorker.registerFunction(WPUtil.FUNC_GETOPTION, ApiaryConfig.postgres, WPGetOption::new);
        apiaryWorker.registerFunction(WPUtil.FUNC_OPTIONEXISTS, ApiaryConfig.postgres, WPOptionExists::new);
        apiaryWorker.registerFunction(WPUtil.FUNC_INSERTOPTION, ApiaryConfig.postgres, WPInsertOption::new);

        if (bugFix != null) {
            // The fixed version.
            if (bugFix.equalsIgnoreCase("comment")) {
                logger.info("Use WordPress bug fix for comment: {}", WPAddCommentFixed.class.getName());
                apiaryWorker.registerFunction(WPUtil.FUNC_ADDCOMMENT, ApiaryConfig.postgres, WPAddCommentFixed::new, true);
            } else if (bugFix.equalsIgnoreCase("option")) {
                logger.info("Use WordPress bug fix for option: {}", WPInsertOptionFixed.class.getName());
                apiaryWorker.registerFunction(WPUtil.FUNC_INSERTOPTION, ApiaryConfig.postgres, WPInsertOptionFixed::new, true);
            }
        } else {
            // The buggy version.
            logger.info("Use WordPress buggy version: {}, {}", WPAddComment.class.getName(), WPInsertOption.class.getName());
        }

        apiaryWorker.startServing();
        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient(dbAddr));
        if (retroMode > 0) {
            long startTime = System.currentTimeMillis();
            RetroBenchmark.retroReplayExec(client.get(), retroMode, startExecId, endExecId);
            long elapsedTime = System.currentTimeMillis() - startTime;
            ApiaryConfig.recordInput = true;  // Record again.
            // TODO: Check inconsistencies and errors.
            apiaryWorker.shutdown();
            logger.info("Replay mode {}, execution time: {} ms", retroMode, elapsedTime);
            return;
        }

        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);
        // Create a client pool.
        BlockingQueue<ApiaryWorkerClient> clientPool = new LinkedBlockingQueue<>();
        for (int i = 0; i < threadPoolSize; i++) {
            clientPool.add(new ApiaryWorkerClient(dbAddr));
        }

        // Add posts, and comments for each post.
        for (int postId = 0; postId < numPosts; postId++) {
            int res = client.get().executeFunction(WPUtil.FUNC_ADDPOST, postId, "benchmark post " + postId).getInt();
            assert (res == 0);
            for (int cid = 0; cid < initCommentsPerPost; cid++) {
                res = client.get().executeFunction(WPUtil.FUNC_ADDCOMMENT, postId, cid, String.format("Post {} comment {}", postId, cid)).getInt();
                assert (res == 0);
            }
        }

        // Try to inject concurrent comments until we find inconsistency.
        int maxTry = 200;
        boolean foundInconsistency = false;
        int i;
        for (i = 0; i < maxTry; i++) {
            Future<Integer> trashFut = threadPool.submit(new WpTask(clientPool, WPOpType.TRASH_POST, null, i, -1, ""));
            Thread.sleep(ThreadLocalRandom.current().nextInt(5));
            int cid = commentId.incrementAndGet();
            Future<Integer> commentFut = threadPool.submit(new WpTask(clientPool, WPOpType.ADD_COMMENT, null, i, cid, String.format("Concurrent comment post {} comment {}", i, cid)));

            int trashRes, commentRes;
            try {
                trashRes = trashFut.get();
                commentRes = commentFut.get();
                assert (i == trashRes);
                assert (commentRes == 0);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }

            // Restore the post.
            int res = client.get().executeFunction(WPUtil.FUNC_UNTRASHPOST, i).getInt();
            String[] resList = client.get().executeFunction(WPUtil.FUNC_GETPOSTCOMMENTS, i).getStringArray();
            assert (resList.length > 1);

            // Check inconssitency.
            resList = client.get().executeFunction(WPUtil.FUNC_COMMENTSTATUS, i).getStringArray();
            if (resList.length > 1) {
                logger.info("Found inconsistency!");
                foundInconsistency = true;
                break;
            }
        }
        if (!foundInconsistency) {
            logger.error("Failed to find inconsistency in posts... exit.");
            threadPool.shutdown();
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
            return;
        }

        // Try to cause option primary key error.
        for (i = 0; i < maxTry; i++) {
            Future<Integer> fut1 = threadPool.submit(new WpTask(clientPool, WPOpType.UPDATE_OPTION, null, "option-" + i, "value0-" + i, "no"));
            // Add arbitrary delay.
            Thread.sleep(ThreadLocalRandom.current().nextInt(2));
            Future<Integer> fut2 = threadPool.submit(new WpTask(clientPool, WPOpType.UPDATE_OPTION, null, "option-" + i, "value1-" + i, "no"));

            int res1, res2;
            try {
                res1 = fut1.get();
                res2 = fut2.get();
                assert (res1 > -2);
                assert (res2 > -2);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }

            // Check the option and make sure it's set, stop if the second one failed.
            String resStr = client.get().executeFunction(WPUtil.FUNC_GETOPTION, "option-" + i).getString();
            assert (resStr.contains("value"));
            if (res2 == -1) {
                logger.info("Found error! Option: {}", i);
                break;
            }
        }

        if (i >= maxTry) {
            logger.error("Failed to find inconsistency in options... exit.");
            threadPool.shutdown();
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
            return;
        }
        // TODO: start actual benchmarks.
        return;
    }

    private static void resetAllTables(String dbAddr) {
        try {
            PostgresConnection pgConn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "dbos");

            pgConn.dropTable(ApiaryConfig.tableFuncInvocations);
            pgConn.dropTable(ApiaryConfig.tableRecordedInputs);
            pgConn.dropTable(ProvenanceBuffer.PROV_ApiaryMetadata);
            pgConn.dropTable(ProvenanceBuffer.PROV_QueryMetadata);
            pgConn.dropTable(WPUtil.WP_POSTS_TABLE);
            pgConn.createTable(WPUtil.WP_POSTS_TABLE, WPUtil.WP_POSTS_SCHEMA);
            pgConn.dropTable(WPUtil.WP_POSTMETA_TABLE);
            pgConn.createTable(WPUtil.WP_POSTMETA_TABLE, WPUtil.WP_POSTMETA_SCHEMA);
            pgConn.dropTable(WPUtil.WP_COMMENTS_TABLE);
            pgConn.createTable(WPUtil.WP_COMMENTS_TABLE, WPUtil.WP_COMMENTS_SCHEMA);
            pgConn.dropTable(WPUtil.WP_OPTIONS_TABLE);
            pgConn.createTable(WPUtil.WP_OPTIONS_TABLE, WPUtil.WP_OPTIONS_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to Postgres.");
            throw new RuntimeException("Failed to connect to Postgres.");
        }
    }

    private static void resetAppTables(String dbAddr) {
        try {
            PostgresConnection pgConn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "dbos");
            pgConn.truncateTable(WPUtil.WP_POSTS_TABLE, false);
            pgConn.truncateTable(WPUtil.WP_POSTMETA_TABLE, false);
            pgConn.truncateTable(WPUtil.WP_COMMENTS_TABLE, false);
            pgConn.truncateTable(WPUtil.WP_OPTIONS_TABLE, false);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to Postgres.");
            throw new RuntimeException("Failed to connect to Postgres.");
        }
    }
}
