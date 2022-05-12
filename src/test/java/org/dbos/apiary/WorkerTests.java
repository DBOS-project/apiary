package org.dbos.apiary;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.procedures.voltdb.tests.StatelessIncrement;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.procedures.voltdb.tests.StatelessDriver;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.dbos.apiary.voltdb.VoltConnection;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.ProcCallException;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WorkerTests {
    private static final Logger logger = LoggerFactory.getLogger(WorkerTests.class);
    @BeforeEach
    public void truncateTables() throws IOException, ProcCallException {
        VoltConnection ctxt = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
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
    public void testFib() throws IOException {
        logger.info("testFib");
        for (int i = 0; i < 10; i++) {
            ApiaryConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
            ApiaryWorker worker = new ApiaryWorker(c, new ApiaryNaiveScheduler(), 128);
            worker.startServing();

            ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

            int res;
            res = client.executeFunction("FibonacciFunction", 1).getInt();
            assertEquals(1, res);

            res = client.executeFunction("FibonacciFunction", 10).getInt();
            assertEquals(55, res);

            res = client.executeFunction("FibonacciFunction", 30).getInt();
            assertEquals(832040, res);

            worker.shutdown();
        }
    }

    @Test
    public void testAddition() throws IOException {
        logger.info("testAddition");
        ApiaryConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(c, new ApiaryNaiveScheduler(), 128);
        worker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        String res = client.executeFunction("AdditionFunction", 1, "2", new String[]{"matei", "zaharia"}, new int[]{2, 3}).getString();
        assertEquals("8mateizaharia", res);

        worker.shutdown();
    }

    @Test
    public void testAsyncClientAddition() throws IOException {
        logger.info("testAsyncClientAddition");
        ApiaryConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(c, new ApiaryNaiveScheduler(), 128);
        worker.startServing();

        ZContext clientContext = new ZContext();
        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost", clientContext);

        ZMQ.Socket socket = client.getSocket("localhost");
        ZMQ.Poller poller = clientContext.createPoller(1);
        poller.register(socket, ZMQ.Poller.POLLIN);

        // Non-blocking send. Then get result and calculate latency.
        long actualSendTime = System.nanoTime();
        byte[] reqBytes = client.serializeExecuteRequest("AdditionFunction", "defaultService", 1, "2", new String[]{"matei", "zaharia"}, new int[]{2, 3});
        for (int i = 0; i < 5; i++) {
            socket.send(reqBytes, 0);
        }

        // Poll and get the results.
        byte[] replyBytes = null;
        int recvCnt = 0;
        while (recvCnt < 5) {
            poller.poll(0);
            if (poller.pollin(0)) {
                ZMsg msg = ZMsg.recvMsg(socket);
                ZFrame content = msg.getLast();
                assertTrue(content != null);
                replyBytes = content.getData();
                msg.destroy();

                ExecuteFunctionReply reply = ExecuteFunctionReply.parseFrom(replyBytes);
                String res = reply.getReplyString();
                assertEquals("8mateizaharia", res);
                long senderTs = reply.getSenderTimestampNano();
                long recvTs = System.nanoTime();
                long elapse = (recvTs - senderTs) / 1000;
                assertTrue(elapse > 0);
                logger.info("Elapsed time: {} μs", elapse);

                long actualElapse = (recvTs - actualSendTime) / 1000;
                logger.info("Actual elapsed time: {} μs", actualElapse);
                recvCnt++;
            }
        }
        poller.close();
        clientContext.close();
        worker.shutdown();
    }


    @Test
    public void testStatelessCounter() throws IOException, InterruptedException {
        logger.info("testStatelessCounter");
        ApiaryConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(c, new ApiaryNaiveScheduler(), 128);
        worker.registerStatelessFunction("StatelessIncrement", StatelessIncrement::new);
        worker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        String res;
        res = client.executeFunction("CounterFunction", "0").getString();
        assertEquals("1", res);

        res = client.executeFunction("CounterFunction", "0").getString();
        assertEquals("2", res);

        res = client.executeFunction("CounterFunction", "1").getString();
        assertEquals("1", res);

        // Should be able to see provenance data if Vertica is running.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        worker.shutdown();
    }

    @Test
    public void testStatelessDriver() throws IOException {
        logger.info("testStatelessDriver");
        ApiaryConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(c, new ApiaryNaiveScheduler(), 128);
        worker.registerStatelessFunction("StatelessDriver", StatelessDriver::new);
        worker.registerStatelessFunction("StatelessIncrement", StatelessIncrement::new);
        worker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("StatelessDriver", "0").getInt();
        assertEquals(1, res);

        res = client.executeFunction("StatelessDriver", "8").getInt();
        assertEquals(55, res);
        worker.shutdown();
    }

    @Test
    public void testSynchronousCounter() throws IOException {
        logger.info("testSynchronousCounter");
        ApiaryConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(c, new ApiaryNaiveScheduler(), 128);
        worker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        String res;
        res = client.executeFunction("SynchronousCounter", "0").getString();
        assertEquals("1", res);

        res = client.executeFunction("SynchronousCounter", "0").getString();
        assertEquals("2", res);

        res = client.executeFunction("SynchronousCounter", "1").getString();
        assertEquals("1", res);

        worker.shutdown();
    }
}
