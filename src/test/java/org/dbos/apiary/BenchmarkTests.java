package org.dbos.apiary;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.procedures.voltdb.retwis.RetwisMerge;
import org.dbos.apiary.procedures.voltdb.retwis.RetwisStatelessGetTimeline;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.voltdb.VoltDBConnection;
import org.dbos.apiary.worker.ApiaryWFQScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.worker.ApiaryWorkerClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.ProcCallException;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class BenchmarkTests {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkTests.class);
    @BeforeEach
    public void truncateTables() throws IOException, ProcCallException {
        VoltDBConnection ctxt = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        ctxt.client.callProcedure("TruncateTables");
    }

    @Test
    public void testRetwis() throws IOException, InterruptedException {
        logger.info("testRetwis");
        ApiaryConnection c = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWFQScheduler scheduler = new ApiaryWFQScheduler();
        ApiaryWorker worker = new ApiaryWorker(c, scheduler, 128);
        worker.registerStatelessFunction("RetwisMerge", RetwisMerge::new);
        worker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient();

        int resInt;
        resInt = client.executeFunction("localhost", "RetwisPost", "defaultService", 0, 0, 0, "hello0").getInt();
        assertEquals(0, resInt);
        resInt = client.executeFunction("localhost", "RetwisPost", "defaultService", 0, 1, 1, "hello1").getInt();
        assertEquals(0, resInt);
        resInt = client.executeFunction("localhost", "RetwisPost", "defaultService", 1, 2, 0, "hello2").getInt();
        assertEquals(1, resInt);
        resInt = client.executeFunction("localhost", "RetwisFollow", "defaultService", 1, 0).getInt();
        assertEquals(1, resInt);
        resInt = client.executeFunction("localhost", "RetwisFollow", "defaultService", 1, 1).getInt();
        assertEquals(1, resInt);

        int[] followees = client.executeFunction("localhost", "RetwisGetFollowees", "defaultService", 1).getIntArray();
        assertEquals(2, followees.length);
        assertTrue(followees[0] == 0 && followees[1] == 1 || followees[0] == 1 && followees[1] == 0);

        String resString;
        resString = client.executeFunction("localhost", "RetwisGetPosts", "defaultService", 0).getString();
        assertEquals("hello0,hello1", resString);
        resString = client.executeFunction("localhost", "RetwisGetTimeline", "defaultService", 1).getString();
        assertEquals(3, resString.split(",").length);
        assertTrue(resString.contains("hello0"));
        assertTrue(resString.contains("hello1"));
        assertTrue(resString.contains("hello2"));

        worker.shutdown();
    }

    @Test
    public void testStatelessRetwis() throws IOException {
        logger.info("testStatelessRetwis");
        ApiaryConnection c = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWFQScheduler scheduler = new ApiaryWFQScheduler();
        ApiaryWorker worker = new ApiaryWorker(c, scheduler, 128);
        worker.registerStatelessFunction("RetwisStatelessGetTimeline", RetwisStatelessGetTimeline::new);
        worker.registerStatelessFunction("RetwisMerge", RetwisMerge::new);
        worker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient();

        int resInt;
        resInt = client.executeFunction("localhost", "RetwisPost", "defaultService",  0, 0, 0, "hello0").getInt();
        assertEquals(0, resInt);
        resInt = client.executeFunction("localhost", "RetwisPost", "defaultService",  0, 1, 1, "hello1").getInt();
        assertEquals(0, resInt);
        resInt = client.executeFunction("localhost", "RetwisPost", "defaultService", 1, 2, 0, "hello2").getInt();
        assertEquals(1, resInt);
        resInt = client.executeFunction("localhost", "RetwisFollow", "defaultService", 1, 0).getInt();
        assertEquals(1, resInt);
        resInt = client.executeFunction("localhost", "RetwisFollow", "defaultService", 1, 1).getInt();
        assertEquals(1, resInt);

        String res = client.executeFunction("localhost", "RetwisStatelessGetTimeline", "defaultService",1).getString();
        assertEquals(3, res.split(",").length);
        assertTrue(res.contains("hello0"));
        assertTrue(res.contains("hello1"));
        assertTrue(res.contains("hello2"));
        worker.shutdown();
    }

    @Test
    public void testIncrement() throws IOException, InterruptedException {
        logger.info("testIncrement");
        ApiaryConnection c = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWFQScheduler scheduler = new ApiaryWFQScheduler();
        ApiaryWorker worker = new ApiaryWorker(c, scheduler, 128);
        worker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient();

        int res;
        res = client.executeFunction("localhost", "IncrementProcedure", "defaultService", 0).getInt();
        assertEquals(1, res);
        res = client.executeFunction("localhost", "IncrementProcedure", "defaultService", 0).getInt();
        assertEquals(2, res);
        res = client.executeFunction("localhost", "IncrementProcedure", "defaultService", 0).getInt();
        assertEquals(3, res);
        res = client.executeFunction("localhost", "IncrementProcedure", "defaultService",  55).getInt();
        assertEquals(1, res);
        worker.shutdown();
    }
}
