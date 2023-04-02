package org.dbos.apiary;

import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.procedures.voltdb.tests.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.voltdb.VoltConnection;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.IOException;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class VoltDBTests {
    private static final Logger logger = LoggerFactory.getLogger(VoltDBTests.class);

    private ApiaryWorker apiaryWorker;

    @BeforeAll
    public static void testConnection() {
        assumeTrue(TestUtils.testVoltConnection());
    }

    @BeforeEach
    public void reset() {
        try {
            VoltConnection ctxt = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
            ctxt.client.callProcedure("TruncateTables");
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to VoltDB.");
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

    @Test
    public void testVoltPartitionInfo() throws IOException {
        logger.info("testVoltPartitionInfo");
        VoltConnection ctxt;
        try {
            ctxt = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        } catch (Exception e) {
            logger.info("No VoltDB instance!");
            return;
        }
        int numPartitions = ctxt.getNumPartitions();
        logger.info("Detected {} partitions.", numPartitions);
        assertTrue(numPartitions > 0);

        HashMap<Integer, String> partitionHostMap = (HashMap)((HashMap)ctxt.getPartitionHostMap()).clone();
        String localhost = InetAddress.getLocalHost().getHostName();
        for (int p : partitionHostMap.keySet()) {
            String hn = partitionHostMap.get(p);
            assertEquals(localhost, hn);
            logger.info("partition {} --> host {}", p, hn);
        }
        assertEquals(numPartitions, partitionHostMap.size());

        // Update and test again.
        ctxt.updatePartitionInfo();
        int numPartitions2 = ctxt.getNumPartitions();
        assertEquals(numPartitions, numPartitions2);
        Map<Integer, String> partitionHostMap2 = ctxt.getPartitionHostMap();
        assertTrue(partitionHostMap.equals(partitionHostMap2));
    }

    @Test
    public void testFib() throws IOException {
        logger.info("testFib");
        for (int i = 0; i < 10; i++) {
            VoltConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
            ApiaryWorker worker = new ApiaryWorker(new ApiaryNaiveScheduler(), 128);
            worker.registerConnection(ApiaryConfig.voltdb, c);
            worker.registerFunction("FibonacciFunction", ApiaryConfig.voltdb, FibonacciFunction::new);
            worker.registerFunction("FibSumFunction", ApiaryConfig.voltdb, FibSumFunction::new);
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
        VoltConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(new ApiaryNaiveScheduler(), 128);
        worker.registerConnection(ApiaryConfig.voltdb, c);
        worker.registerFunction("AdditionFunction", ApiaryConfig.voltdb, AdditionFunction::new);
        worker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        String res = client.executeFunction("AdditionFunction", 1, "2", new String[]{"matei", "zaharia"}, new int[]{2, 3}).getString();
        assertEquals("8mateizaharia", res);

        worker.shutdown();
    }

    @Test
    public void testAsyncClientAddition() throws IOException {
        logger.info("testAsyncClientAddition");
        VoltConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(new ApiaryNaiveScheduler(), 128);
        worker.registerConnection(ApiaryConfig.voltdb, c);
        worker.registerFunction("AdditionFunction", ApiaryConfig.voltdb, AdditionFunction::new);
        worker.startServing();

        ZContext clientContext = new ZContext();
        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost", clientContext);

        ZMQ.Socket socket = client.getSocket("localhost");
        ZMQ.Poller poller = clientContext.createPoller(1);
        poller.register(socket, ZMQ.Poller.POLLIN);

        // Non-blocking send. Then get result and calculate latency.
        long actualSendTime = System.nanoTime();
        byte[] reqBytes = client.serializeExecuteRequest("AdditionFunction", ApiaryConfig.defaultRole, 1, "2", new String[]{"matei", "zaharia"}, new int[]{2, 3});
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
        VoltConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(new ApiaryNaiveScheduler(), 128);
        worker.registerConnection(ApiaryConfig.voltdb, c);
        worker.registerFunction("StatelessIncrement", ApiaryConfig.stateless, StatelessIncrement::new);
        worker.registerFunction("CounterFunction", ApiaryConfig.voltdb, CounterFunction::new);
        worker.registerFunction("InsertFunction", ApiaryConfig.voltdb, InsertFunction::new);
        worker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        String res;
        res = client.executeFunction("CounterFunction", "0").getString();
        assertEquals("1", res);

        res = client.executeFunction("CounterFunction", "0").getString();
        assertEquals("2", res);

        res = client.executeFunction("CounterFunction", "1").getString();
        assertEquals("1", res);

        worker.shutdown();
    }

    @Test
    public void testStatelessDriver() throws IOException {
        logger.info("testStatelessDriver");
        VoltConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(new ApiaryNaiveScheduler(), 128);
        worker.registerConnection(ApiaryConfig.voltdb, c);
        worker.registerFunction("FibonacciFunction", ApiaryConfig.voltdb, FibonacciFunction::new);
        worker.registerFunction("FibSumFunction", ApiaryConfig.voltdb, FibSumFunction::new);
        worker.registerFunction("StatelessDriver", ApiaryConfig.stateless, StatelessDriver::new);
        worker.registerFunction("StatelessIncrement", ApiaryConfig.stateless, StatelessIncrement::new);
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
        VoltConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(new ApiaryNaiveScheduler(), 128);
        worker.registerConnection(ApiaryConfig.voltdb, c);
        worker.registerFunction("SynchronousCounter", ApiaryConfig.voltdb, SynchronousCounter::new);
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

    @Test
    public void testVoltProvenance() throws IOException, SQLException, InterruptedException {
        logger.info("testVoltProvenance");
        VoltConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.vertica, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.voltdb, c);
        apiaryWorker.registerFunction("VoltProvenanceBasic", ApiaryConfig.voltdb, VoltProvenanceBasic::new);
        apiaryWorker.startServing();

        ProvenanceBuffer provBuff = apiaryWorker.workerContext.provBuff;
        if (provBuff == null) {
            logger.info("Provenance buffer (Vertica) not available.");
            return;
        }

        // Wait a bit so previous provenance capture data would be flushed out.
        Thread.sleep(ProvenanceBuffer.exportInterval * 4);
        Connection verticaConn = provBuff.conn.get();
        Statement stmt = verticaConn.createStatement();
        String[] tables = {"FUNCINVOCATIONS", "KVTABLEEVENTS"};
        for (String table : tables) {
            stmt.execute(String.format("TRUNCATE TABLE %s;", table));
        }

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        int key = 10, value = 100;

        res = client.executeFunction("VoltProvenanceBasic", key, value).getInt();
        assertEquals(101, res);

        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        // Check function invocation table.
        String table = "FUNCINVOCATIONS";
        ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY APIARY_EXPORT_TIMESTAMP DESC;", table));
        rs.next();
        long txid1 = rs.getLong(1);
        long resExecId = rs.getLong(3);
        String resFuncName = rs.getString(5);
        long expectedID = ((long)client.getClientID() << 48);
        assertEquals(expectedID, resExecId);
        assertEquals(VoltProvenanceBasic.class.getName(), resFuncName);

        rs.next();
        long txid2 = rs.getLong(1);
        resExecId = rs.getLong(3);
        resFuncName = rs.getString(5);
        assertEquals(expectedID, resExecId);
        assertEquals(VoltProvenanceBasic.class.getName(), resFuncName);

        // Inner transaction should have the same transaction ID.
        assertTrue(txid1 > 0L);
        assertEquals(txid1, txid2);

        // Check KVTable.
        table = "KVTABLEEVENTS";
        rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY APIARY_EXPORT_TIMESTAMP;", table));
        rs.next();

        // Should be an insert for basevalue=1.
        long resTxid = rs.getLong(1);
        int resExportOp = rs.getInt(3);
        int resKey = rs.getInt(4);
        int resValue = rs.getInt(5);
        assertEquals(txid2, resTxid);
        assertEquals(ProvenanceBuffer.ExportOperation.INSERT.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(1, resValue);

        // Should be an insert for the key value.
        rs.next();
        resTxid = rs.getLong(1);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(3);
        resKey = rs.getInt(4);
        resValue = rs.getInt(5);
        assertEquals(ProvenanceBuffer.ExportOperation.INSERT.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(value, resValue);

        // Should be a read.
        rs.next();
        resTxid = rs.getLong(1);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(3);
        resKey = rs.getInt(4);
        resValue = rs.getInt(5);
        assertEquals(ProvenanceBuffer.ExportOperation.READ.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(100, resValue);
    }
}
