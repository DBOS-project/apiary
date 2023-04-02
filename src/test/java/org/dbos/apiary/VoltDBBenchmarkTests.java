package org.dbos.apiary;

import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.procedures.voltdb.increment.IncrementProcedure;
import org.dbos.apiary.procedures.voltdb.retwis.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.voltdb.VoltConnection;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class VoltDBBenchmarkTests {
    private static final Logger logger = LoggerFactory.getLogger(VoltDBBenchmarkTests.class);

    @BeforeAll
    public static void testConnection() {
        assumeTrue(TestUtils.testVoltConnection());
    }

    @BeforeEach
    public void truncateTables() {
        try {
            VoltConnection ctxt = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
            ctxt.client.callProcedure("TruncateTables");
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to VoltDB.");
            assumeTrue(false);
        }
    }

    @Test
    public void testRetwis() throws IOException {
        logger.info("testRetwis");
        VoltConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);

        ApiaryWorker worker = new ApiaryWorker(new ApiaryNaiveScheduler(), 128);
        worker.registerConnection(ApiaryConfig.voltdb, c);
        worker.registerFunction("RetwisMerge", ApiaryConfig.stateless, RetwisMerge::new);
        worker.registerFunction("RetwisPost", ApiaryConfig.voltdb, RetwisPost::new);
        worker.registerFunction("RetwisFollow", ApiaryConfig.voltdb, RetwisFollow::new);
        worker.registerFunction("RetwisGetFollowees", ApiaryConfig.voltdb, RetwisGetFollowees::new);
        worker.registerFunction("RetwisGetPosts", ApiaryConfig.voltdb, RetwisGetPosts::new);
        worker.registerFunction("RetwisGetTimeline", ApiaryConfig.voltdb, RetwisGetTimeline::new);
        worker.startServing();

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

        int[] followees = client.executeFunction("RetwisGetFollowees", 1).getIntArray();
        assertEquals(2, followees.length);
        assertTrue(followees[0] == 0 && followees[1] == 1 || followees[0] == 1 && followees[1] == 0);

        String resString;
        resString = client.executeFunction("RetwisGetPosts", 0).getString();
        assertEquals("hello0,hello1", resString);
        resString = client.executeFunction("RetwisGetTimeline", 1).getString();
        assertEquals(3, resString.split(",").length);
        assertTrue(resString.contains("hello0"));
        assertTrue(resString.contains("hello1"));
        assertTrue(resString.contains("hello2"));

        worker.shutdown();
    }

    @Test
    public void testStatelessRetwis() throws IOException {
        logger.info("testStatelessRetwis");
        VoltConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(new ApiaryNaiveScheduler(), 128);
        worker.registerConnection(ApiaryConfig.voltdb, c);
        worker.registerFunction("RetwisMerge", ApiaryConfig.stateless, RetwisMerge::new);
        worker.registerFunction("RetwisStatelessGetTimeline", ApiaryConfig.stateless, RetwisStatelessGetTimeline::new);
        worker.registerFunction("RetwisPost", ApiaryConfig.voltdb, RetwisPost::new);
        worker.registerFunction("RetwisFollow", ApiaryConfig.voltdb, RetwisFollow::new);
        worker.registerFunction("RetwisGetFollowees", ApiaryConfig.voltdb, RetwisGetFollowees::new);
        worker.registerFunction("RetwisGetPosts", ApiaryConfig.voltdb, RetwisGetPosts::new);
        worker.registerFunction("RetwisGetTimeline", ApiaryConfig.voltdb, RetwisGetTimeline::new);
        worker.startServing();

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

        String res = client.executeFunction("RetwisStatelessGetTimeline", 1).getString();
        assertEquals(3, res.split(",").length);
        assertTrue(res.contains("hello0"));
        assertTrue(res.contains("hello1"));
        assertTrue(res.contains("hello2"));
        worker.shutdown();
    }

    @Test
    public void testIncrement() throws IOException {
        logger.info("testIncrement");
        VoltConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(new ApiaryNaiveScheduler(), 128);
        worker.registerConnection(ApiaryConfig.voltdb, c);
        worker.registerFunction("IncrementProcedure", ApiaryConfig.voltdb, IncrementProcedure::new);
        worker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("IncrementProcedure", 0).getInt();
        assertEquals(1, res);
        res = client.executeFunction("IncrementProcedure", 0).getInt();
        assertEquals(2, res);
        res = client.executeFunction("IncrementProcedure", 0).getInt();
        assertEquals(3, res);
        res = client.executeFunction("IncrementProcedure", 55).getInt();
        assertEquals(1, res);
        worker.shutdown();
    }
}
