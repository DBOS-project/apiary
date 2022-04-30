package org.dbos.apiary;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.interposition.ProvenanceBuffer;
import org.dbos.apiary.procedures.voltdb.tests.AdditionFunction;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.voltdb.VoltDBConnection;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.worker.ApiaryWorkerClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.ProcCallException;

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
        VoltDBConnection ctxt = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
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
        // TODO: implement more tests as we have read/write provenance capture.
        logger.info("testVoltProvenance");
        ApiaryConnection c = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
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
        String[] tables = {"FUNCINVOCATIONS"};
        for (String table : tables) {
            stmt.execute(String.format("TRUNCATE TABLE %s;", table));
        }

        ApiaryWorkerClient client = new ApiaryWorkerClient();

        String res = client.executeFunction("localhost", "AdditionFunction", "testVoltProvService", 1, "2", new String[]{"aaa", "bbb"}, new int[]{2, 3}).getString();
        assertEquals("8aaabbb", res);

        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        // Check function invocation table.
        String table = "FUNCINVOCATIONS";
        ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY APIARY_EXPORT_TIMESTAMP;", table));
        rs.next();
        long txid = rs.getLong(1);
        long resExecId = rs.getLong(3);
        String resService = rs.getString(4);
        String resFuncName = rs.getString(5);
        assertTrue(txid > 0l);
        assertEquals(0l, resExecId);
        assertEquals(resService, "testVoltProvService");
        assertEquals(AdditionFunction.class.getName(), resFuncName);
    }
}
