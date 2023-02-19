package org.dbos.apiary.benchmarks.retro;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.benchmarks.RetroBenchmark;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.moodle.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class MoodleBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(MoodleBenchmark.class);

    private static final int threadPoolSize = 128;
    private static final int numWorker = 64;

    // Users can subscribe to forums.
    private static final int numUsers = 5000;
    private static final int numForums = 1000;

    private static final int threadWarmupMs = 5000;  // First 5 seconds of request would be warm-up requests.

    private static final Collection<Long> readTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> writeTimes = new ConcurrentLinkedQueue<>();

    private static ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient("localhost"));

    private static final int initialUserId = 123;
    private static final int initialForumId = 555;

    static class SubsTask implements Callable<Integer> {
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

    public static void benchmark(String dbAddr, Integer interval, Integer duration, boolean skipLoad, int retroMode, long startExecId, long endExecId, String bugFix, List<Integer> percentages) throws SQLException, InterruptedException, ExecutionException, InvalidProtocolBufferException {
        ApiaryConfig.isolationLevel = ApiaryConfig.REPEATABLE_READ;
        int readPercentage = percentages.get(0); // Use the first one.
        boolean hasProv = ApiaryConfig.recordInput ? true : false;  // Enable provenance?

        if (retroMode == ApiaryConfig.ReplayMode.NOT_REPLAY.getValue()) {
            if (!skipLoad) {
                // Only reset tables if we do initial runs.
                resetAllTables(dbAddr);
            }
        } else {
            ApiaryConfig.recordInput = false;
            ApiaryConfig.captureFuncInvocations = true;
            if (!skipLoad){
                // TODO: for now, we just drop entire data tables. We can probably use point-in-time recovery, or recover through our selective replay.
                resetAppTables(dbAddr);
            }
        }

        PostgresConnection pgConn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "dbos", RetroBenchmark.provenanceDB, RetroBenchmark.provenanceAddr);

        ApiaryWorker apiaryWorker;
        if (hasProv) {
            // Enable provenance logging in the worker.
            apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numWorker, RetroBenchmark.provenanceDB, RetroBenchmark.provenanceAddr);
        } else {
            // Disable provenance.
            apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numWorker);
        }
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pgConn);

        if ((bugFix != null) && bugFix.equalsIgnoreCase("subscribe")) {
            logger.info("Use Moodle bug fix: {}", MDLSubscribeTxn.class.getName());
            // Use the bug fix: transactional version.
            apiaryWorker.registerFunction(MDLUtil.FUNC_IS_SUBSCRIBED, ApiaryConfig.postgres, MDLSubscribeTxn::new, true);
        } else {
            // The buggy version.
            logger.info("Use Moodle buggy version: {}", MDLIsSubscribed.class.getName());
            apiaryWorker.registerFunction(MDLUtil.FUNC_IS_SUBSCRIBED, ApiaryConfig.postgres, MDLIsSubscribed::new, false);
            apiaryWorker.registerFunctionSet(MDLUtil.FUNC_IS_SUBSCRIBED, MDLUtil.FUNC_IS_SUBSCRIBED, MDLUtil.FUNC_FORUM_INSERT);
        }
        apiaryWorker.registerFunction(MDLUtil.FUNC_FORUM_INSERT, ApiaryConfig.postgres, MDLForumInsert::new, false);
        apiaryWorker.registerFunction(MDLUtil.FUNC_FETCH_SUBSCRIBERS, ApiaryConfig.postgres, MDLFetchSubscribers::new, false);
        apiaryWorker.registerFunction(MDLUtil.FUNC_LOAD_DATA, ApiaryConfig.postgres, MDLLoadData::new, false);

        apiaryWorker.startServing();

        if (retroMode > 0) {
            long startTime = System.currentTimeMillis();
            RetroBenchmark.retroReplayExec(client.get(), retroMode, startExecId, endExecId);
            long elapsedTime = System.currentTimeMillis() - startTime;
            ApiaryConfig.captureFuncInvocations = false;
            int[] resList = client.get().executeFunction(MDLUtil.FUNC_FETCH_SUBSCRIBERS, initialForumId).getIntArray();
            if (resList.length > 1) {
                logger.info("Replay found duplications!");
            } else {
                logger.info("Replay found no duplications.");
            }
            apiaryWorker.shutdown();
            logger.info("Replay mode {}, execution time: {} ms", retroMode, elapsedTime);
            return;
        }

        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);

        // Insert duplicated entries.
        List<SubsTask> tasks = new ArrayList<>();
        tasks.add(new SubsTask(initialUserId, initialForumId));
        tasks.add(new SubsTask(initialUserId, initialForumId));
        List<Future<Integer>> futures = threadPool.invokeAll(tasks);
        for (Future<Integer> future : futures) {
            if (!future.isCancelled()) {
                int res = future.get();
                assert (res != -1);
            }
        }

        // Check subscriptions.
        int[] resList = client.get().executeFunction(MDLUtil.FUNC_FETCH_SUBSCRIBERS, initialForumId).getIntArray();
        assert (resList.length > 1);

        // Insert initial subscriptions.
        int[] loadForums = new int[numForums];
        int[] loadUsers = new int[numForums];
        for (int i = 0; i < numForums; i++) {
            loadForums[i] = i;
            loadUsers[i] = 0; // Initial user 0;
        }
        int loadedRows = client.get().executeFunction(MDLUtil.FUNC_LOAD_DATA, loadUsers, loadForums).getInt();
        logger.info("Loaded {} rows of data.", loadedRows);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (duration * 1000 + threadWarmupMs);

        Runnable r = () -> {
            long t0 = System.nanoTime();
            int chooser = ThreadLocalRandom.current().nextInt(100);
            if (chooser < readPercentage) {
                // Check the list of subscribers of a random forum.
                int forumId = ThreadLocalRandom.current().nextInt(0, numForums);
                try {
                    client.get().executeFunction(MDLUtil.FUNC_FETCH_SUBSCRIBERS, forumId).getIntArray();
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
                readTimes.add(System.nanoTime() - t0);
            } else {
                // Insert a subscription for a random user + forum.
                int userId = ThreadLocalRandom.current().nextInt(1, numUsers);
                int forumId = ThreadLocalRandom.current().nextInt(0, numForums);
                try {
                    int res = client.get().executeFunction(MDLUtil.FUNC_IS_SUBSCRIBED, userId, forumId).getInt();
                    assert (res == userId);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
                writeTimes.add(System.nanoTime() - t0);
            }
        };

        while (System.currentTimeMillis() < endTime) {
            long t = System.nanoTime();
            if (System.currentTimeMillis() - startTime < threadWarmupMs) {
                readTimes.clear();
                writeTimes.clear();
            }
            threadPool.submit(r);
            while (System.nanoTime() - t < interval.longValue() * 1000) {
                // Busy-spin
            }
        }

        long elapsedTime = (System.currentTimeMillis() - startTime) - threadWarmupMs;

        threadPool.shutdownNow();
        threadPool.awaitTermination(10, TimeUnit.SECONDS);

        double totalThroughput = 0.0;

        List<Long> queryTimes = readTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        int numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            totalThroughput += throughput;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("Forum Reads: Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No reads.");
        }

        queryTimes = writeTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            totalThroughput += throughput;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("Forum Writes: Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No writes");
        }
        logger.info("Total Throughput: {}", totalThroughput);

        apiaryWorker.shutdown();
    }

    private static void resetAllTables(String dbAddr) {
        try {
            PostgresConnection pgConn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "dbos", RetroBenchmark.provenanceDB, RetroBenchmark.provenanceAddr);
            Connection provConn = pgConn.provConnection.get();
            pgConn.dropTable(ProvenanceBuffer.PROV_ApiaryMetadata);
            pgConn.dropTable(MDLUtil.MDL_FORUMSUBS_TABLE);
            pgConn.createTable(MDLUtil.MDL_FORUMSUBS_TABLE, MDLUtil.MDL_FORUM_SCHEMA);
            pgConn.createIndex(MDLUtil.MDL_FORUMID_INDEX);
            PostgresConnection.dropTable(provConn, ApiaryConfig.tableFuncInvocations);
            PostgresConnection.dropTable(provConn, ProvenanceBuffer.PROV_QueryMetadata);
            PostgresConnection.dropTable(provConn, ApiaryConfig.tableRecordedInputs);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to Postgres.");
        }
    }

    private static void resetAppTables(String dbAddr) {
        try {
            PostgresConnection pgConn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "dbos", RetroBenchmark.provenanceDB, RetroBenchmark.provenanceAddr);
            pgConn.dropTable(MDLUtil.MDL_FORUMSUBS_TABLE);
            pgConn.createTable(MDLUtil.MDL_FORUMSUBS_TABLE, MDLUtil.MDL_FORUM_SCHEMA);
            pgConn.createIndex(MDLUtil.MDL_FORUMID_INDEX);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to Postgres.");
        }
    }
}
