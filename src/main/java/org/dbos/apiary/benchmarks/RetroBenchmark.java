package org.dbos.apiary.benchmarks;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.moodle.*;
import org.dbos.apiary.procedures.postgres.moodle.MDLSubscribeTxn;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class RetroBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(RetroBenchmark.class);

    private static final int threadPoolSize = 4;
    private static final int numWorker = 4;

    // Users can subscribe to forums.
    private static final int numUsers = 100;
    private static final int numForums = 10;

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
                res = client.get().executeFunction("MDLIsSubscribed", userId, forumId).getInt();
            } catch (Exception e) {
                res = -1;
            }
            return res;
        }
    }

    public static void benchmark(String dbAddr, Integer interval, Integer duration, int percentageRead, int percentageWrite, boolean skipLoad, int replayMode, long targetExecId) throws SQLException, InterruptedException, ExecutionException, InvalidProtocolBufferException {

        ApiaryConfig.isolationLevel = ApiaryConfig.SERIALIZABLE;

        if (replayMode == ApiaryConfig.ReplayMode.NOT_REPLAY.getValue()) {
            ApiaryConfig.recordInput = true;
            if (!skipLoad) {
                // Only reset tables if we do initial runs.
                resetAllTables(dbAddr);
            }
        } else {
            ApiaryConfig.recordInput = false;
            if (replayMode == ApiaryConfig.ReplayMode.ALL.getValue()){
                // TODO: a better way to restore the database.
                resetAppTables(dbAddr);
            }
        }

        assert (percentageRead + percentageWrite == 100);

        PostgresConnection pgConn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "dbos");

        ApiaryWorker apiaryWorker;
        if (ApiaryConfig.captureUpdates || ApiaryConfig.captureReads) {
            // Enable provenance logging in the worker.
            apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numWorker, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        } else {
            // Disable provenance.
            apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numWorker);
        }
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pgConn);

        if (replayMode != ApiaryConfig.ReplayMode.ALL.getValue()) {
            // The buggy version.
            apiaryWorker.registerFunction("MDLIsSubscribed", ApiaryConfig.postgres, MDLIsSubscribed::new);
            apiaryWorker.registerFunction("MDLForumInsert", ApiaryConfig.postgres, MDLForumInsert::new);
        } else {
            // The transactional version.
            apiaryWorker.registerFunction("MDLIsSubscribed", ApiaryConfig.postgres, MDLSubscribeTxn::new);
        }
        apiaryWorker.registerFunction("MDLFetchSubscribers", ApiaryConfig.postgres, MDLFetchSubscribers::new);
        apiaryWorker.startServing();

        if (replayMode > 0) {
            long startTime = System.currentTimeMillis();
            replayExec(replayMode, targetExecId);
            long elapsedTime = System.currentTimeMillis() - startTime;
            ApiaryConfig.recordInput = true;  // Record again.
            int[] resList = client.get().executeFunction("MDLFetchSubscribers", initialForumId).getIntArray();
            if (resList.length > 1) {
                logger.info("Replay found duplications!");
            } else {
                logger.info("Replay found no duplications.");
            }
            apiaryWorker.shutdown();
            logger.info("Replay mode {}, execution time: {} ms", replayMode, elapsedTime);
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
        int[] resList = client.get().executeFunction("MDLFetchSubscribers", initialForumId).getIntArray();
        assert (resList.length > 1);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (duration * 1000 + threadWarmupMs);

        Runnable r = () -> {
            long t0 = System.nanoTime();
            int chooser = ThreadLocalRandom.current().nextInt(100);
            if (chooser < percentageRead) {
                // Check the list of subscribers of a random forum.
                int forumId = ThreadLocalRandom.current().nextInt(0, numForums);
                try {
                    client.get().executeFunction("MDLFetchSubscribers", forumId).getIntArray();
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
                readTimes.add(System.nanoTime() - t0);
            } else {
                // Insert a subscription for a random user + forum.
                int userId = ThreadLocalRandom.current().nextInt(0, numUsers);
                int forumId = ThreadLocalRandom.current().nextInt(0, numForums);
                try {
                    int res = client.get().executeFunction("MDLIsSubscribed", userId, forumId).getInt();
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

        List<Long> queryTimes = readTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        int numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
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
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("Forum Writes: Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No writes");
        }

        threadPool.shutdown();
        threadPool.awaitTermination(100000, TimeUnit.SECONDS);
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);  // Wait for all entries to be exported.
        apiaryWorker.shutdown();
    }

    private static void replayExec(int replayMode, long targetExecId) throws InvalidProtocolBufferException {
        if (replayMode == ApiaryConfig.ReplayMode.SINGLE.getValue()) {
            // Replay a single execution.
            int res = client.get().replayFunction(targetExecId, "MDLIsSubscribed", initialUserId, initialForumId).getInt();
            assert (res == initialUserId);
        } else if (replayMode == ApiaryConfig.ReplayMode.ALL.getValue()){
            FunctionOutput res = client.get().retroReplay(targetExecId);
            assert (res != null);
        } else {
            logger.error("Do not support replay mode {}", replayMode);
        }
    }

    private static void resetAllTables(String dbAddr) {
        try {
            PostgresConnection pgConn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "dbos");

            pgConn.dropTable("ForumSubscription");
            pgConn.createTable("ForumSubscription", "UserId integer NOT NULL, ForumId integer NOT NULL");
            pgConn.dropTable(ApiaryConfig.tableFuncInvocations);
            pgConn.dropTable(ProvenanceBuffer.PROV_ApiaryMetadata);
            pgConn.dropTable(ProvenanceBuffer.PROV_QueryMetadata);
            pgConn.dropTable(ApiaryConfig.tableRecordedInputs);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to Postgres.");
        }
    }

    private static void resetAppTables(String dbAddr) {
        try {
            PostgresConnection pgConn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "dbos");
            pgConn.truncateTable("ForumSubscription", false);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to Postgres.");
        }
    }
}
