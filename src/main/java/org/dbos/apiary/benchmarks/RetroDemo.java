package org.dbos.apiary.benchmarks;

import com.google.protobuf.InvalidProtocolBufferException;
import org.checkerframework.checker.units.qual.A;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.replay.PostgresFetchSubscribers;
import org.dbos.apiary.procedures.postgres.replay.PostgresForumSubscribe;
import org.dbos.apiary.procedures.postgres.replay.PostgresIsSubscribed;
import org.dbos.apiary.procedures.postgres.retro.PostgresIsSubscribedTxn;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RetroDemo {
    private static final Logger logger = LoggerFactory.getLogger(RetroDemo.class);

    private static final int threadPoolSize = 4;
    private static final int numWorker = 4;

    // Users can subscribe to forums.
    private static final int numUsers = 10;
    private static final int numForums = 10;
    private static final int totalRequests = 10;

    private static final int initialUserId = 11;
    private static final int initialForumId = 22;

    public enum DemoMode {
        NOT_REPLAY(0),
        REPLAY(1),
        RETRO(2);

        private int value;

        private DemoMode (int value) {
            this.value = value;
        }

        public int getValue() {
            return this.value;
        }
    }

    public static void benchmark(String dbAddr, int replayMode, long targetExecId) throws SQLException, InvalidProtocolBufferException, InterruptedException {
        ApiaryConfig.isolationLevel = ApiaryConfig.SERIALIZABLE;
        ApiaryConfig.XDBTransactions = false;
        ApiaryConfig.captureUpdates = true;
        ApiaryConfig.captureReads = true;

        if (replayMode == DemoMode.NOT_REPLAY.getValue()) {
            logger.info("Non-replay mode.");
            ApiaryConfig.recordInput = true;
            // Reset all tables if we do initial runs.
            resetAllTables(dbAddr);
        } else {
            if (replayMode == DemoMode.REPLAY.getValue()) {
                logger.info("Replay mode. Replay the original trace.");
            } else {
                logger.info("Retroactive programming mode. Replay with the bug fix code.");
            }
            ApiaryConfig.recordInput = false;
            // Only reset the application table.
            resetAllTables(dbAddr);
        }

        PostgresConnection pgConn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "dbos");

        ApiaryWorker apiaryWorker;
        // Enable provenance logging in the worker.
        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numWorker, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pgConn);

        if (replayMode != DemoMode.RETRO.getValue()) {
            // The buggy version.
            apiaryWorker.registerFunction("PostgresIsSubscribed", ApiaryConfig.postgres, PostgresIsSubscribed::new);
            apiaryWorker.registerFunction("PostgresForumSubscribe", ApiaryConfig.postgres, PostgresForumSubscribe::new);
        } else {
            // The transactional version.
            apiaryWorker.registerFunction("PostgresIsSubscribed", ApiaryConfig.postgres, PostgresIsSubscribedTxn::new);
        }

        apiaryWorker.registerFunction("PostgresFetchSubscribers", ApiaryConfig.postgres, PostgresFetchSubscribers::new);
        apiaryWorker.startServing();

        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient(dbAddr));

        // Replay, or retro replay.
        if (replayMode != DemoMode.NOT_REPLAY.getValue()) {
            int[] retroResList = client.get().retroReplay(targetExecId).getIntArray();
            if (retroResList.length < 1) {
                logger.error("Replay failed.");
            } else {
                logger.info("Replay finished!");
            }
            apiaryWorker.shutdown();
            Thread.sleep(ProvenanceBuffer.exportInterval * 2);  // Wait for all entries to be exported.
            return;
        }

        // Otherwise, generate the initial execution trace.
        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);
        AtomicInteger execCnt = new AtomicInteger(0);

        Runnable r = () -> {
            try {
                int cnt = execCnt.getAndIncrement();
                if (cnt < 2) {
                    // Insert a subscription for the initial user + forum.
                    int res = client.get().executeFunction("PostgresIsSubscribed", initialUserId, initialForumId).getInt();
                    assert (res == initialUserId);
                } else {
                    // Insert a subscription for random user + forum.
                    int userId = ThreadLocalRandom.current().nextInt(0, numUsers);
                    int forumId = ThreadLocalRandom.current().nextInt(0, numForums);
                    int res = client.get().executeFunction("PostgresIsSubscribed", userId, forumId).getInt();
                    assert (res == userId);
                }
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        };

        // Submit duplicated concurrent requests.
        threadPool.submit(r);
        threadPool.submit(r);

        // Submit other requests.
        for (int i = 0; i < totalRequests; i++) {
            threadPool.submit(r);
            Thread.sleep(ThreadLocalRandom.current().nextInt(10));
        }

        // Clean up.
        threadPool.shutdown();
        threadPool.awaitTermination(10, TimeUnit.SECONDS);

        // Finally, fetch subscribers list for the initial forum.
        int[] resList = client.get().executeFunction("PostgresFetchSubscribers", initialForumId).getIntArray();
        if (resList.length <= 1) {
            logger.error("Failed to generate duplication for forum {}", initialForumId);
        }

        Thread.sleep(ProvenanceBuffer.exportInterval * 2);  // Wait for all entries to be exported.
        apiaryWorker.shutdown();
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
