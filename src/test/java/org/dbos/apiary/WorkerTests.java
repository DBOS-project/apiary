package org.dbos.apiary;

import org.dbos.apiary.executor.ApiaryConnection;
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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WorkerTests {
    private static final Logger logger = LoggerFactory.getLogger(WorkerTests.class);
    @BeforeEach
    public void truncateTables() throws IOException, ProcCallException {
        VoltDBConnection ctxt = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        ctxt.client.callProcedure("TruncateTables");
    }

    @Test
    public void testSerialization() {
        logger.info("testSerialization");
        String[] s = new String[]{"asdf", "jkl;"};
        String[] s2 = Utilities.byteArrayToStringArray(Utilities.stringArraytoByteArray(s));
        for (int i = 0; i < s2.length; i++) {
            assertEquals(s[i], s2[i]);
        }
    }

    @Test
    public void testFib() throws IOException, InterruptedException {
        logger.info("testFib");
        for (int i = 0; i < 100; i++) {
            ApiaryConnection c = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
            ApiaryWorker worker = new ApiaryWorker(8000, c, Map.of(0L, "localhost:8000"), 1);
            worker.startServing();

            ZContext clientContext = new ZContext();
            ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);

            String rep;
            rep = client.executeFunction("localhost:8000", "FibonacciFunction", ApiaryConfig.defaultPkey, "1");
            assertEquals("1", rep);

            rep = client.executeFunction("localhost:8000", "FibonacciFunction", ApiaryConfig.defaultPkey, "10");
            assertEquals("55", rep);

            rep = client.executeFunction("localhost:8000", "FibonacciFunction", ApiaryConfig.defaultPkey, "30");
            assertEquals("832040", rep);
            clientContext.close();
            worker.shutdown();
        }
    }
}
