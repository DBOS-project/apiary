package org.dbos.apiary;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.procedures.voltdb.tests.StatelessIncrement;
import org.dbos.apiary.procedures.voltdb.tests.VoltProvenanceBasic;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.voltdb.VoltConnection;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.client.InternalApiaryWorkerClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.ProcCallException;
import org.zeromq.ZContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

public class VoltDBTests {
    private static final Logger logger = LoggerFactory.getLogger(VoltDBTests.class);

    private ApiaryWorker apiaryWorker;

    @BeforeEach
    public void reset() throws IOException, ProcCallException {
        VoltConnection ctxt = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        ctxt.client.callProcedure("TruncateTables");
        apiaryWorker = null;
    }

    @AfterEach
    public void cleanupWorker() {
        if (apiaryWorker != null) {
            apiaryWorker.shutdown();
        }
    }

    @Test
    public void testVoltProvenance() throws IOException, SQLException, InterruptedException {
        logger.info("testVoltProvenance");
        ApiaryConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        apiaryWorker = new ApiaryWorker(c, new ApiaryNaiveScheduler(), 4);
        apiaryWorker.startServing();

        ProvenanceBuffer provBuff = apiaryWorker.provenanceBuffer;
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
        String resService = rs.getString(4);
        String resFuncName = rs.getString(5);
        long expectedID = ((long)client.getClientID() << 48);
        assertEquals(expectedID, resExecId);
        assertEquals("DefaultService", resService);
        assertEquals(VoltProvenanceBasic.class.getName(), resFuncName);

        rs.next();
        long txid2 = rs.getLong(1);
        resExecId = rs.getLong(3);
        resService = rs.getString(4);
        resFuncName = rs.getString(5);
        assertEquals(expectedID, resExecId);
        assertEquals("DefaultService", resService);
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

    @Test
    public void testExactlyOnceVoltSyncCounter() throws IOException {
        logger.info("testExactlyOnceVoltSyncCounter");
        ApiaryConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(c, new ApiaryNaiveScheduler(), 4);
        worker.startServing();

        InternalApiaryWorkerClient client = new InternalApiaryWorkerClient(new ZContext());

        String res;
        res = client.executeFunction("localhost", "SynchronousCounter", "defaultService", 10, "0").getString();
        assertEquals("1", res);

        res = client.executeFunction("localhost", "SynchronousCounter", "defaultService", 11, "0").getString();
        assertEquals("2", res);

        res = client.executeFunction("localhost", "SynchronousCounter", "defaultService", 12, "1").getString();
        assertEquals("1", res);

        res = client.executeFunction("localhost", "SynchronousCounter", "defaultService", 12, "1").getString();
        assertEquals("1", res);

        worker.shutdown();
    }

    @Test
    public void testExactlyOnceVoltStatelessCounter() throws IOException {
        logger.info("testExactlyOnceVoltStatelessCounter");
        ApiaryConnection c = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        ApiaryWorker worker = new ApiaryWorker(c, new ApiaryNaiveScheduler(), 4);
        worker.registerStatelessFunction("StatelessIncrement", StatelessIncrement::new);
        worker.startServing();

        InternalApiaryWorkerClient client = new InternalApiaryWorkerClient(new ZContext());

        String res;
        res = client.executeFunction("localhost", "CounterFunction", "defaultService", 20, "0").getString();
        assertEquals("1", res);

        res = client.executeFunction("localhost", "CounterFunction", "defaultService", 21, "0").getString();
        assertEquals("2", res);

        res = client.executeFunction("localhost", "CounterFunction", "defaultService", 22, "1").getString();
        assertEquals("1", res);

        res = client.executeFunction("localhost", "CounterFunction", "defaultService", 22, "1").getString();
        assertEquals("1", res);

        worker.shutdown();
    }
}
