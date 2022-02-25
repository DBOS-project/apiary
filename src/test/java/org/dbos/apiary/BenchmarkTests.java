package org.dbos.apiary;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.procedures.stateless.Increment;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.dbos.apiary.voltdb.VoltDBConnection;
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
        ApiaryWorker worker = new ApiaryWorker(c);
        worker.startServing();

        ZContext clientContext = new ZContext();
        ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);

        String res;
        res = client.executeFunction("localhost", "RetwisPost", 0, "0", "0", "0", "hello0");
        assertEquals("0", res);
        res = client.executeFunction("localhost", "RetwisPost", 0, "0", "1", "1", "hello1");
        assertEquals("0", res);
        res = client.executeFunction("localhost", "RetwisFollow", 1, "1", "0");
        assertEquals("1", res);
        res = client.executeFunction("localhost", "RetwisGetPosts", 0, "0");
        assertEquals("hello0,hello1", res);
        clientContext.close();
        worker.shutdown();
    }
}
