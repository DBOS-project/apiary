package org.dbos.apiary;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
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

public class WorkerTests {
    private static final Logger logger = LoggerFactory.getLogger(WorkerTests.class);
    @BeforeEach
    public void truncateTables() throws IOException, ProcCallException {
        VoltDBConnection ctxt = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        ctxt.client.callProcedure("TruncateTables");
    }

    @Test
    public void workerTest() throws IOException, InterruptedException {
        for (int i = 0; i < 100; i++) {
            logger.info("workerTest");
            ApiaryConnection c = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
            ApiaryWorker worker = new ApiaryWorker(8000, c);
            worker.startServing();

            ZContext clientContext = new ZContext();
            ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);

            String rep = client.executeFunction("matei", null, "localhost:8000");
            assertEquals("matei!!!", rep);
            clientContext.close();
            worker.shutdown();
        }
    }
}
