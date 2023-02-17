package org.dbos.apiary;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.moodle.*;
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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class PostgresTests {
    private static final Logger logger = LoggerFactory.getLogger(PostgresTests.class);

    private ApiaryWorker apiaryWorker;

    // Local provenance config.
    private static final int provenancePort = ApiaryConfig.postgresPort;
    private static final String provenanceDB = ApiaryConfig.postgres;
    private static final String provenanceAddr = "localhost";

    @BeforeAll
    public static void testConnection() {
        assumeTrue(TestUtils.testPostgresConnection());
        // Set the isolation level to serializable.
        ApiaryConfig.isolationLevel = ApiaryConfig.SERIALIZABLE;

        // Disable XDB transactions.
        ApiaryConfig.XDBTransactions = false;
        ApiaryConfig.provenancePort = provenancePort;
    }

    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");
            conn.dropTable(ApiaryConfig.tableFuncInvocations);
            conn.dropTable(ApiaryConfig.tableRecordedInputs);
            conn.dropTable("KVTable");
            conn.createTable("KVTable", "KVKey integer PRIMARY KEY NOT NULL, KVValue integer NOT NULL");
            conn.dropTable("RetwisPosts");
            conn.createTable("RetwisPosts", "UserID integer NOT NULL, PostID integer NOT NULL, Timestamp integer NOT NULL, Post varchar(1000) NOT NULL");
            conn.dropTable("RetwisFollowees");
            conn.createTable("RetwisFollowees", "UserID integer NOT NULL, FolloweeID integer NOT NULL");
            conn.dropTable("ForumSubscription");
            conn.createTable("ForumSubscription", "UserId integer NOT NULL, ForumId integer NOT NULL");
            conn.dropTable("BigIntTable");
            conn.createTable("BigIntTable", "col1 BIGINT NOT NULL, col2 BIGINT NOT NULL, col3 BIGINT NOT NULL");
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

    @Test void testInsertMany() throws SQLException, InvalidProtocolBufferException {
        logger.info("testInsertMany");
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("PostgresInsertMany", ApiaryConfig.postgres, PostgresInsertMany::new);
        apiaryWorker.registerFunction("PostgresCountTable", ApiaryConfig.postgres, PostgresCountTable::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        int numRows = 10;
        int[] numbers = new int[numRows];
        for (int i = 0; i < numRows; i++) {
            numbers[i] = i + 1000;
        }

        res = client.executeFunction("PostgresInsertMany", "BigIntTable", 3, numbers).getInt();
        assertEquals(numRows, res);

        res = client.executeFunction("PostgresCountTable", "BigIntTable").getInt();
        assertEquals(numRows, res);
    }

    @Test
    public void testForumSubscribe() throws SQLException, InvalidProtocolBufferException {
        logger.info("testForumSubscribe");
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("MDLIsSubscribed", ApiaryConfig.postgres, MDLIsSubscribed::new);
        apiaryWorker.registerFunction("MDLForumInsert", ApiaryConfig.postgres, MDLForumInsert::new);
        apiaryWorker.registerFunction("MDLFetchSubscribers", ApiaryConfig.postgres, MDLFetchSubscribers::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("MDLIsSubscribed", 123, 555).getInt();
        assertEquals(123, res);

        // Subscribe again, should return the same userId.
        res = client.executeFunction("MDLIsSubscribed", 123, 555).getInt();
        assertEquals(123, res);

        // Get a list of subscribers, should only contain one user entry.
        int[] resList = client.executeFunction("MDLFetchSubscribers",555).getIntArray();
        assertEquals(1, resList.length);
        assertEquals(123, resList[0]);
    }

    @Test
    public void testForumSubscribeConcurrent() throws SQLException, InvalidProtocolBufferException, InterruptedException, ExecutionException {
        // Run until duplications happen.
        logger.info("testForumSubscribeConcurrent");
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("MDLIsSubscribed", ApiaryConfig.postgres, MDLIsSubscribed::new);
        apiaryWorker.registerFunction("MDLForumInsert", ApiaryConfig.postgres, MDLForumInsert::new);
        apiaryWorker.registerFunction("MDLFetchSubscribers", ApiaryConfig.postgres, MDLFetchSubscribers::new);
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
                    res = client.get().executeFunction("MDLIsSubscribed", userId, forumId).getInt();
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
            int[] resList = client.get().executeFunction("MDLFetchSubscribers", i+maxTry).getIntArray();
            if (resList.length > 1) {
                logger.info("Found duplications! User: {}, Forum: {}", i, i+maxTry);
                break;
            }
        }

        threadPool.shutdown();
    }

    @Test
    public void testFibPostgres() throws InvalidProtocolBufferException, SQLException {
        logger.info("testFibPostgres");

        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
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

        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
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
}
