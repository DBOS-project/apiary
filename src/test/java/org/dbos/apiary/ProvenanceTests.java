package org.dbos.apiary;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.tests.PostgresProvenanceBasic;
import org.dbos.apiary.procedures.postgres.tests.PostgresProvenanceJoins;
import org.dbos.apiary.procedures.postgres.tests.PostgresProvenanceMultiRows;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class ProvenanceTests {
    private static final Logger logger = LoggerFactory.getLogger(ProvenanceTests.class);

    private ApiaryWorker apiaryWorker;

    // Local provenance config.
    private static final int provenancePort = ApiaryConfig.postgresPort;
    private static final String provenanceDB = ApiaryConfig.postgres;
    private static final String provenanceAddr = "localhost";

    @BeforeAll
    public static void testConnection() {
        // Set the isolation level to serializable.
        ApiaryConfig.isolationLevel = ApiaryConfig.SERIALIZABLE;
        // Disable XDB transactions.
        ApiaryConfig.XDBTransactions = false;

        assumeTrue(TestUtils.testPostgresConnection());
        ApiaryConfig.recordInput = true;
        ApiaryConfig.captureMetadata = true;
        ApiaryConfig.provenancePort = provenancePort;
    }

    @BeforeEach
    public void resetTables() {
        try {
            ApiaryConfig.captureReads = true;
            ApiaryConfig.captureUpdates = true;
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos", provenanceDB, provenanceAddr);
            Connection provConn = conn.provConnection.get();
            PostgresConnection.dropTable(provConn, ApiaryConfig.tableFuncInvocations);
            PostgresConnection.dropTable(provConn, ApiaryConfig.tableRecordedInputs);
            PostgresConnection.dropTable(provConn, ProvenanceBuffer.PROV_QueryMetadata);
            conn.dropTable(ProvenanceBuffer.PROV_ApiaryMetadata);
            conn.dropTable("KVTable");
            conn.createTable("KVTable", "KVKey integer PRIMARY KEY NOT NULL, KVValue integer NOT NULL");
            conn.dropTable("KVTableTwo");
            conn.createTable("KVTableTwo", "KVKeyTwo integer PRIMARY KEY NOT NULL, KVValueTwo integer NOT NULL");
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to Postgres.");
            assumeTrue(false);
        }
        apiaryWorker = null;
    }

    @AfterEach
    public void cleanUpWorker() {
        if (apiaryWorker != null) {
            apiaryWorker.shutdown();
        }
    }

    @Test
    public void testProvenanceBuffer() throws InterruptedException, ClassNotFoundException, SQLException {
        logger.info("testProvenanceBuffer");
        ProvenanceBuffer buf = new ProvenanceBuffer(provenanceDB, provenanceAddr);
        String table = ApiaryConfig.tableFuncInvocations;

        // Wait until previous exporter finished.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
        PostgresConnection pgconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos", provenanceDB, provenanceAddr);
        Connection conn = pgconn.provConnection.get();
        Statement stmt = conn.createStatement();

        // Add something to function invocation log table.
        long txid = 1234l;
        long timestamp = 3456789l;
        long executionID = 456l;
        long funcID = 1l;
        String role = "testRole";
        String funcName = "testFunction";
        buf.addEntry(table, txid, timestamp, executionID, funcID, 0, role, funcName);

        long txid2 = 2222l;
        long timestamp2 = 456789l;
        long executionID2 = 789l;
        long funcID2 = 2l;
        buf.addEntry(table, txid2, timestamp2, executionID2, funcID2, 1, role, funcName);
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY %s;", table, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID));
        int cnt = 0;
        while (rs.next()) {
            long resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
            long resTimestamp = rs.getLong(ProvenanceBuffer.PROV_APIARY_TIMESTAMP);
            long resExecId = rs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
            String resRole = rs.getString(ProvenanceBuffer.PROV_APIARY_ROLE);
            String resFuncName = rs.getString(ProvenanceBuffer.PROV_PROCEDURENAME);
            int isreplayed = rs.getShort(ProvenanceBuffer.PROV_ISREPLAY);
            long funcId = rs.getLong(ProvenanceBuffer.PROV_FUNCID);
            if (cnt == 0) {
                assertEquals(txid, resTxid);
                assertEquals(timestamp, resTimestamp);
                assertEquals(executionID, resExecId);
                assertTrue(funcName.equals(resFuncName));
                assertEquals(0, isreplayed);
                assertEquals(1l, funcId);
            } else {
                assertEquals(txid2, resTxid);
                assertEquals(timestamp2, resTimestamp);
                assertEquals(executionID2, resExecId);
                assertTrue(funcName.equals(resFuncName));
                assertEquals(1, isreplayed);
                assertEquals(2l, funcId);
            }
            assertTrue(role.equals(resRole));

            cnt++;
        }
        assertEquals(2, cnt);
        buf.close();
        conn.close();
    }

    @Test
    public void testPostgresProvenance() throws InvalidProtocolBufferException, SQLException, InterruptedException {
        logger.info("testPostgresProvenance");

        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("PostgresProvenanceBasic", ApiaryConfig.postgres, PostgresProvenanceBasic::new);
        apiaryWorker.startServing();

        ProvenanceBuffer provBuff = apiaryWorker.workerContext.provBuff;
        assert(provBuff != null);

        // Wait a bit so previous provenance capture data would be flushed out.
        Thread.sleep(ProvenanceBuffer.exportInterval * 4);
        Connection provConn = provBuff.conn.get();
        Statement stmt = provConn.createStatement();
        String[] tables = {"FUNCINVOCATIONS", "KVTableEvents"};
        for (String table : tables) {
            stmt.execute(String.format("TRUNCATE TABLE %s;", table));
        }

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        int key = 10, value = 100;
        res = client.executeFunction("PostgresProvenanceBasic", key, value).getInt();
        assertEquals(101, res);

        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        // Check provenance tables.
        // Check function invocation table.
        String table = ApiaryConfig.tableFuncInvocations;
        ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY %s DESC;", table, ProvenanceBuffer.PROV_APIARY_TIMESTAMP));
        rs.next();
        long txid1 = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        long resExecId = rs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
        String resRole = rs.getString(ProvenanceBuffer.PROV_APIARY_ROLE);
        String resFuncName = rs.getString(ProvenanceBuffer.PROV_PROCEDURENAME);
        assertEquals(ApiaryConfig.defaultRole, resRole);
        assertEquals("PostgresProvenanceBasic", resFuncName);

        rs.next();
        long txid2 = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        resExecId = rs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
        resRole = rs.getString(ProvenanceBuffer.PROV_APIARY_ROLE);
        resFuncName = rs.getString(ProvenanceBuffer.PROV_PROCEDURENAME);
        assertEquals(ApiaryConfig.defaultRole, resRole);
        assertEquals("PostgresProvenanceBasic", resFuncName);

        // Inner transaction should have the same transaction ID.
        assertEquals(txid1, txid2);

        // Check KVTable.
        table = "KVTableEvents";
        int expectedSeqNum = 1;  // The first one returns no value.
        rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY %s;", table, ProvenanceBuffer.PROV_APIARY_TIMESTAMP));

        rs.next();
        // Should be an insert for key=1.
        long resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        int resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        int resKey = rs.getInt("KVKey");
        int resValue = rs.getInt("KVValue");
        int resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(expectedSeqNum, resSeqNum);
        expectedSeqNum += 1;
        assertEquals(txid2, resTxid);
        assertEquals(ProvenanceBuffer.ExportOperation.INSERT.getValue(), resExportOp);
        assertEquals(1, resKey);
        assertEquals(value, resValue);

        // Should be an insert for the key value.
        rs.next();
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(expectedSeqNum, resSeqNum);
        expectedSeqNum += 1;
        assertEquals(ProvenanceBuffer.ExportOperation.INSERT.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(value, resValue);

        // Should be a read.
        rs.next();
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(expectedSeqNum, resSeqNum);
        expectedSeqNum += 1;
        assertEquals(ProvenanceBuffer.ExportOperation.READ.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(100, resValue);

        // Should be an update.
        rs.next();
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(expectedSeqNum, resSeqNum);
        expectedSeqNum += 1;
        assertEquals(ProvenanceBuffer.ExportOperation.UPDATE.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(value+1, resValue);

        // Should be a read again.
        rs.next();
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(expectedSeqNum, resSeqNum);
        expectedSeqNum += 1;
        assertEquals(ProvenanceBuffer.ExportOperation.READ.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(101, resValue);

        // Should be a delete.
        rs.next();
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(expectedSeqNum, resSeqNum);
        assertEquals(ProvenanceBuffer.ExportOperation.DELETE.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(value+1, resValue);
    }

    @Test
    public void testPostgresProvenanceJoins() throws InvalidProtocolBufferException, SQLException, InterruptedException {
        logger.info("testPostgresProvenanceJoins");

        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("PostgresProvenanceJoins", ApiaryConfig.postgres, PostgresProvenanceJoins::new);
        apiaryWorker.startServing();

        ProvenanceBuffer provBuff = apiaryWorker.workerContext.provBuff;
        assert(provBuff != null);

        Thread.sleep(ProvenanceBuffer.exportInterval * 4);
        Connection provConn = provBuff.conn.get();
        Statement stmt = provConn.createStatement();
        String[] tables = {"FUNCINVOCATIONS", "KVTableEvents", "KVTableTwoEvents"};
        for (String table : tables) {
            stmt.execute(String.format("TRUNCATE TABLE %s;", table));
        }

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresProvenanceJoins", 1, 2, 3).getInt();
        assertEquals(5, res);

        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        // Check KVTable.
        String table = "KVTableEvents";
        ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY %s;", table, ProvenanceBuffer.PROV_APIARY_TIMESTAMP));
        rs.next();

        // Should be an insert for key=1.
        int resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        int resKey = rs.getInt("KVKey");
        int resValue = rs.getInt("KVValue");
        int resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(0, resSeqNum);
        assertEquals(ProvenanceBuffer.ExportOperation.INSERT.getValue(), resExportOp);
        assertEquals(1, resKey);
        assertEquals(2, resValue);

        // Should be a read.
        rs.next();
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(2, resSeqNum);
        assertEquals(ProvenanceBuffer.ExportOperation.READ.getValue(), resExportOp);
        assertEquals(2, resValue);

        // Check KVTableTwo.
        table = "KVTableTwoEvents";
        rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY %s;", table, ProvenanceBuffer.PROV_APIARY_TIMESTAMP));
        rs.next();

        // Should be an insert for key=1.
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKeyTwo");
        resValue = rs.getInt("KVValueTwo");
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(1, resSeqNum);
        assertEquals(ProvenanceBuffer.ExportOperation.INSERT.getValue(), resExportOp);
        assertEquals(1, resKey);
        assertEquals(3, resValue);

        // Should be a read.
        rs.next();
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKeyTwo");
        resValue = rs.getInt("KVValueTwo");
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        assertEquals(2, resSeqNum);
        assertEquals(ProvenanceBuffer.ExportOperation.READ.getValue(), resExportOp);
        assertEquals(3, resValue);

        // Check Query Metadata table.
        String metatable = ProvenanceBuffer.PROV_QueryMetadata;
        int expectedSeqNum = 0;
        rs = stmt.executeQuery(String.format("SELECT * FROM %s WHERE %s != 'apiarymetadata' ORDER BY %s;", metatable, ProvenanceBuffer.PROV_QUERY_TABLENAMES, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID));
        rs.next();

        // The first one should be an insert to KVTable.
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        String resTableName = rs.getString(ProvenanceBuffer.PROV_QUERY_TABLENAMES);
        String resProjection = rs.getString(ProvenanceBuffer.PROV_QUERY_PROJECTION);
        String resQueryString = rs.getString(ProvenanceBuffer.PROV_QUERY_STRING);
        assertEquals(expectedSeqNum, resSeqNum);
        expectedSeqNum += 1;
        assertEquals("kvtable", resTableName);
        assertEquals("*", resProjection);
        assertTrue(resQueryString.startsWith("INSERT INTO KVTABLE(KVKEY, KVVALUE)"));


        rs.next();
        // The next one should be an insert to KVTableTwo.
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        resTableName = rs.getString(ProvenanceBuffer.PROV_QUERY_TABLENAMES);
        resProjection = rs.getString(ProvenanceBuffer.PROV_QUERY_PROJECTION);
        resQueryString = rs.getString(ProvenanceBuffer.PROV_QUERY_STRING);
        assertEquals(expectedSeqNum, resSeqNum);
        expectedSeqNum += 1;
        assertEquals("kvtabletwo", resTableName);
        assertEquals("*", resProjection);
        assertTrue(resQueryString.startsWith("INSERT INTO KVTABLETWO(KVKEYTWO, KVVALUETWO)"));

        rs.next();
        // The next one should be a read to both tables.
        resSeqNum = rs.getInt(ProvenanceBuffer.PROV_QUERY_SEQNUM);
        resTableName = rs.getString(ProvenanceBuffer.PROV_QUERY_TABLENAMES);
        resProjection = rs.getString(ProvenanceBuffer.PROV_QUERY_PROJECTION);
        resQueryString = rs.getString(ProvenanceBuffer.PROV_QUERY_STRING);
        assertEquals(expectedSeqNum, resSeqNum);
        assertEquals("kvtable,kvtabletwo", resTableName);
        assertEquals("kvvalue,kvvaluetwo", resProjection);
        assertTrue(resQueryString.equalsIgnoreCase("SELECT KVValue, KVValueTWO FROM KVTable, KVTableTwo WHERE KVKey = KVKeyTwo"));
    }

    @Test
    public void testPostgresProvenanceMultiRows() throws InvalidProtocolBufferException, SQLException, InterruptedException {
        logger.info("testPostgresProvenanceMultiRows");

        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("PostgresProvenanceMultiRows", ApiaryConfig.postgres, PostgresProvenanceMultiRows::new);
        apiaryWorker.startServing();

        ProvenanceBuffer provBuff = apiaryWorker.workerContext.provBuff;
        assert(provBuff != null);

        // Wait a bit so previous provenance capture data would be flushed out.
        Thread.sleep(ProvenanceBuffer.exportInterval * 4);
        Connection provConn = provBuff.conn.get();
        Statement stmt = provConn.createStatement();
        String[] tables = {"FUNCINVOCATIONS", "KVTableEvents"};
        for (String table : tables) {
            stmt.execute(String.format("TRUNCATE TABLE %s;", table));
        }

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        int key1 = 10, value1 = 100;
        int key2 = 20, value2 = 11;
        res = client.executeFunction("PostgresProvenanceMultiRows", key1, value1, key2, value2).getInt();
        assertEquals(111, res);

        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        // Check provenance tables.
        // Check function invocation table.
        String table = "FUNCINVOCATIONS";
        ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY %s DESC;", table, ProvenanceBuffer.PROV_APIARY_TIMESTAMP));
        rs.next();
        long txid1 = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        long resExecId = rs.getLong(ProvenanceBuffer.PROV_EXECUTIONID);
        String resRole = rs.getString(ProvenanceBuffer.PROV_APIARY_ROLE);
        String resFuncName = rs.getString(ProvenanceBuffer.PROV_PROCEDURENAME);
        assertEquals(ApiaryConfig.defaultRole, resRole);
        assertEquals("PostgresProvenanceMultiRows", resFuncName);

        // Check KVTable.
        table = "KVTableEvents";
        rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY %s, KVKEY;", table, ProvenanceBuffer.PROV_APIARY_TIMESTAMP));
        rs.next();

        // Should be an insert for key1.
        long resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        int resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        int resKey = rs.getInt("KVKey");
        int resValue = rs.getInt("KVValue");
        assertEquals(txid1, resTxid);
        assertEquals(ProvenanceBuffer.ExportOperation.INSERT.getValue(), resExportOp);
        assertEquals(key1, resKey);
        assertEquals(value1, resValue);

        // Should be an insert for the key2.
        rs.next();
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        assertEquals(ProvenanceBuffer.ExportOperation.INSERT.getValue(), resExportOp);
        assertEquals(key2, resKey);
        assertEquals(value2, resValue);

        // Should be a read for key1.
        rs.next();
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        assertEquals(ProvenanceBuffer.ExportOperation.READ.getValue(), resExportOp);
        assertEquals(key1, resKey);
        assertEquals(value1, resValue);

        // Should be a read again for key2.
        rs.next();
        resTxid = rs.getLong(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID);
        assertEquals(txid1, resTxid);
        resExportOp = rs.getInt(ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE);
        resKey = rs.getInt("KVKey");
        resValue = rs.getInt("KVValue");
        assertEquals(ProvenanceBuffer.ExportOperation.READ.getValue(), resExportOp);
        assertEquals(key2, resKey);
        assertEquals(value2, resValue);
    }
}
