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
import org.zeromq.ZContext;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        ApiaryWorker worker = new ApiaryWorker(c, scheduler);
        worker.registerStatelessFunction("RetwisMerge", RetwisMerge::new);
        worker.startServing();

        ZContext clientContext = new ZContext();
        ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);

        String res;
        res = client.executeFunction("localhost", "RetwisPost", "defaultService", "0", "0", "0", "hello0");
        assertEquals("0", res);
        res = client.executeFunction("localhost", "RetwisPost", "defaultService", "0", "1", "1", "hello1");
        assertEquals("0", res);
        res = client.executeFunction("localhost", "RetwisPost", "defaultService", "1", "2", "0", "hello2");
        assertEquals("1", res);
        res = client.executeFunction("localhost", "RetwisFollow", "defaultService", "1", "0");
        assertEquals("1", res);
        res = client.executeFunction("localhost", "RetwisFollow", "defaultService", "1", "1");
        assertEquals("1", res);
        res = client.executeFunction("localhost", "RetwisGetPosts", "defaultService", "0");
        assertEquals("hello0,hello1", res);
        res = client.executeFunction("localhost", "RetwisGetTimeline", "defaultService", "1");
        assertEquals(3, res.split(",").length);
        assertTrue(res.contains("hello0"));
        assertTrue(res.contains("hello1"));
        assertTrue(res.contains("hello2"));
        clientContext.close();
        worker.shutdown();
    }

    @Test
    public void testStatelessRetwis() throws IOException, InterruptedException {
        logger.info("testStatelessRetwis");
        ApiaryConnection c = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWFQScheduler scheduler = new ApiaryWFQScheduler();
        ApiaryWorker worker = new ApiaryWorker(c, scheduler);
        worker.registerStatelessFunction("RetwisStatelessGetTimeline", RetwisStatelessGetTimeline::new);
        worker.startServing();

        ZContext clientContext = new ZContext();
        ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);

        String res;
        res = client.executeFunction("localhost", "RetwisPost", "defaultService", "0", "0", "0", "hello0");
        assertEquals("0", res);
        res = client.executeFunction("localhost", "RetwisPost", "defaultService", "0", "1", "1", "hello1");
        assertEquals("0", res);
        res = client.executeFunction("localhost", "RetwisPost", "defaultService", "1", "2", "0", "hello2");
        assertEquals("1", res);
        res = client.executeFunction("localhost", "RetwisFollow", "defaultService", "1", "0");
        assertEquals("1", res);
        res = client.executeFunction("localhost", "RetwisFollow", "defaultService", "1", "1");
        assertEquals("1", res);
        res = client.executeFunction("localhost", "RetwisGetFollowees", "defaultService", "1");
        assertEquals(2, res.split(",").length);
        assertTrue(res.contains("0"));
        assertTrue(res.contains("1"));
        res = client.executeFunction("localhost", "RetwisStatelessGetTimeline", "defaultService", "1");
        assertEquals(3, res.split(",").length);
        assertTrue(res.contains("hello0"));
        assertTrue(res.contains("hello1"));
        assertTrue(res.contains("hello2"));
        clientContext.close();
        worker.shutdown();
    }

    @Test
    public void testIncrement() throws IOException, InterruptedException {
        logger.info("testIncrement");
        ApiaryConnection c = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWFQScheduler scheduler = new ApiaryWFQScheduler();
        ApiaryWorker worker = new ApiaryWorker(c, scheduler);
        worker.startServing();

        ZContext clientContext = new ZContext();
        ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);

        String res;
        res = client.executeFunction("localhost", "IncrementProcedure", "defaultService", "0");
        assertEquals("1", res);
        res = client.executeFunction("localhost", "IncrementProcedure", "defaultService", "0");
        assertEquals("2", res);
        res = client.executeFunction("localhost", "IncrementProcedure", "defaultService", "0");
        assertEquals("3", res);
        res = client.executeFunction("localhost", "IncrementProcedure", "defaultService", "55");
        assertEquals("1", res);
        clientContext.close();
        worker.shutdown();
    }
}
