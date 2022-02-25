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
        assertEquals(s.length, s2.length);
        for (int i = 0; i < s2.length; i++) {
            assertEquals(s[i], s2[i]);
        }
        int[] is = new int[]{1, 2, 3, 4, 3, 2, 1, 11234};
        int[] is2 = Utilities.byteArrayToIntArray(Utilities.intArrayToByteArray(is));
        assertEquals(is.length, is2.length);
        for (int i = 0; i < is2.length; i++) {
            assertEquals(is[i], is2[i]);
        }
    }

    @Test
    public void testFib() throws IOException, InterruptedException {
        logger.info("testFib");
        for (int i = 0; i < 100; i++) {
            ApiaryConnection c = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
            ApiaryWorker worker = new ApiaryWorker(c);
            worker.startServing();

            ZContext clientContext = new ZContext();
            ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);

            String res;
            res = client.executeFunction("localhost", "FibonacciFunction", ApiaryConfig.defaultPkey, "1");
            assertEquals("1", res);

            res = client.executeFunction("localhost", "FibonacciFunction", ApiaryConfig.defaultPkey, "10");
            assertEquals("55", res);

            res = client.executeFunction("localhost", "FibonacciFunction", ApiaryConfig.defaultPkey, "30");
            assertEquals("832040", res);
            clientContext.close();
            worker.shutdown();
        }
    }

    @Test
    public void testAddition() throws IOException, InterruptedException {
        logger.info("testAddition");
        ApiaryConnection c = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(c);
        worker.startServing();

        ZContext clientContext = new ZContext();
        ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);

        String res = client.executeFunction("localhost", "AdditionFunction", ApiaryConfig.defaultPkey, "1", "2", new String[]{"matei", "zaharia"});
        assertEquals("3mateizaharia", res);

        clientContext.close();
        worker.shutdown();
    }

    @Test
    public void testStatelessCounter() throws IOException, InterruptedException {
        logger.info("testStatelessIncrement");
        ApiaryConnection c = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(c);
        worker.registerStatelessFunction("increment", Increment::new);
        worker.startServing();

        ZContext clientContext = new ZContext();
        ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);

        String res;
        res = client.executeFunction("localhost", "CounterFunction", ApiaryConfig.defaultPkey, "0");
        assertEquals("1", res);

        res = client.executeFunction("localhost", "CounterFunction", ApiaryConfig.defaultPkey, "0");
        assertEquals("2", res);

        res = client.executeFunction("localhost", "CounterFunction", 1, "1");
        assertEquals("1", res);

        clientContext.close();
        worker.shutdown();
    }

    @Test
    public void testSynchronousCounter() throws IOException, InterruptedException {
        logger.info("testSynchronousCounter");
        ApiaryConnection c = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(c);
        worker.startServing();

        ZContext clientContext = new ZContext();
        ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);

        String res;
        res = client.executeFunction("localhost", "SynchronousCounter", ApiaryConfig.defaultPkey, "0");
        assertEquals("1", res);

        res = client.executeFunction("localhost", "SynchronousCounter", ApiaryConfig.defaultPkey, "0");
        assertEquals("2", res);

        res = client.executeFunction("localhost", "SynchronousCounter", 1, "1");
        assertEquals("1", res);

        clientContext.close();
        worker.shutdown();
    }
}
