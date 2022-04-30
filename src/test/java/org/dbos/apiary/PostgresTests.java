package org.dbos.apiary;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.interposition.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.tests.*;
import org.dbos.apiary.procedures.postgres.retwis.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.worker.ApiaryWorkerClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class PostgresTests {
    private static final Logger logger = LoggerFactory.getLogger(PostgresTests.class);

    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection ctxt = new PostgresConnection("localhost", ApiaryConfig.postgresPort);
            ctxt.dropTable("KVTable");
            ctxt.createTable("KVTable", "(KVKey integer PRIMARY KEY NOT NULL, KVValue integer NOT NULL)");
            ctxt.dropTable("RetwisPosts");
            ctxt.createTable("RetwisPosts", "(UserID integer NOT NULL, PostID integer NOT NULL, Timestamp integer NOT NULL, Post varchar(1000) NOT NULL)");
            ctxt.dropTable("RetwisFollowees");
            ctxt.createTable("RetwisFollowees", "(UserID integer NOT NULL, FolloweeID integer NOT NULL)");
        } catch (Exception e) {
            logger.info("Failed to connect to Postgres.");
        }
    }

    @Test
    public void testFibPostgres() throws InvalidProtocolBufferException {
        logger.info("testFibPostgres");

        PostgresConnection conn;
        try {
            conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort);
        } catch (Exception e) {
            logger.info("No Postgres instance!");
            return;
        }
        conn.registerFunction("PostgresFibonacciFunction", PostgresFibonacciFunction::new);
        conn.registerFunction("PostgresFibSumFunction", PostgresFibSumFunction::new);

        ApiaryWorker worker = new ApiaryWorker(conn, new ApiaryNaiveScheduler(), 4);
        worker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient();

        int res;
        res = client.executeFunction("localhost", "PostgresFibonacciFunction", "defaultService", 1).getInt();
        assertEquals(1, res);

        res = client.executeFunction("localhost", "PostgresFibonacciFunction", "defaultService", 6).getInt();
        assertEquals(8, res);

        res = client.executeFunction("localhost", "PostgresFibonacciFunction", "defaultService", 10).getInt();
        assertEquals(55, res);

        worker.shutdown();
    }

    @Test
    public void testRetwisPostgres() throws InvalidProtocolBufferException {
        logger.info("testRetwisPostgres");

        PostgresConnection conn;
        try {
            conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort);
        } catch (Exception e) {
            logger.info("No Postgres instance!");
            return;
        }
        conn.registerFunction("RetwisPost", RetwisPost::new);
        conn.registerFunction("RetwisFollow", RetwisFollow::new);
        conn.registerFunction("RetwisGetPosts", RetwisGetPosts::new);
        conn.registerFunction("RetwisGetFollowees", RetwisGetFollowees::new);
        conn.registerFunction("RetwisGetTimeline", RetwisGetTimeline::new);

        ApiaryWorker worker = new ApiaryWorker(conn, new ApiaryNaiveScheduler(), 4);
        worker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient();

        int resInt;
        resInt = client.executeFunction("localhost", "RetwisPost", "defaultService", 0, 0, 0, "hello0").getInt();
        assertEquals(0, resInt);
        resInt = client.executeFunction("localhost", "RetwisPost", "defaultService", 0, 1, 1, "hello1").getInt();
        assertEquals(0, resInt);
        resInt = client.executeFunction("localhost", "RetwisPost", "defaultService", 1, 2, 0, "hello2").getInt();
        assertEquals(1, resInt);
        resInt = client.executeFunction("localhost", "RetwisFollow", "defaultService", 1, 0).getInt();
        assertEquals(1, resInt);
        resInt = client.executeFunction("localhost", "RetwisFollow", "defaultService", 1, 1).getInt();
        assertEquals(1, resInt);

        String[] postResult = client.executeFunction("localhost", "RetwisGetPosts", "defaultService", 0).getStringArray();
        assertArrayEquals(new String[]{"hello0", "hello1"}, postResult);

        int[] followees = client.executeFunction("localhost", "RetwisGetFollowees", "defaultService", 1).getIntArray();
        assertEquals(2, followees.length);
        assertTrue(followees[0] == 0 && followees[1] == 1 || followees[0] == 1 && followees[1] == 0);

        String[] timeline = client.executeFunction("localhost", "RetwisGetTimeline", "defaultService", 1).getStringArray();
        assertTrue(Arrays.asList(timeline).contains("hello0"));
        assertTrue(Arrays.asList(timeline).contains("hello1"));
        assertTrue(Arrays.asList(timeline).contains("hello2"));

        worker.shutdown();
    }

    @Test
    public void testPostgresProvenance() throws InvalidProtocolBufferException, SQLException, InterruptedException {
        logger.info("testPostgresProvenance");

        PostgresConnection conn;
        try {
            conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort);
        } catch (Exception e) {
            logger.info("No Postgres instance!");
            return;
        }
        conn.registerFunction("ProvenanceTestFunction", ProvenanceTestFunction::new);

        ApiaryWorker worker = new ApiaryWorker(conn, new ApiaryNaiveScheduler(), 1);
        worker.startServing();

        ProvenanceBuffer provBuff = worker.provenanceBuffer;
        if (provBuff == null) {
            logger.info("Provenance buffer (Vertica) not available.");
            worker.shutdown();
            return;
        }

        Connection verticaConn = provBuff.conn.get();
        Statement stmt = verticaConn.createStatement();
        String[] tables = {"FUNCINVOCATIONS", "KVTABLE"};
        for (String table : tables) {
            stmt.execute(String.format("TRUNCATE TABLE %s;", table));
        }

        ApiaryWorkerClient client = new ApiaryWorkerClient();

        int res;
        int key = 10, value = 100;
        res = client.executeFunction("localhost", "ProvenanceTestFunction", "testProvService", key, value).getInt();
        assertEquals(101, res);

        Thread.sleep(ProvenanceBuffer.exportInterval * 2);

        // Check provenance tables.
        // Check function invocation table.
        String table = "FUNCINVOCATIONS";
        ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY APIARY_TRANSACTION_ID;", table));
        rs.next();
        long resExecId = rs.getLong(3);
        String resService = rs.getString(4);
        String resFuncName = rs.getString(5);
        assertEquals(0l, resExecId);
        assertEquals(resService, "testProvService");
        assertEquals(ProvenanceTestFunction.class.getName(), resFuncName);

        // Check KVTable.
        table = "KVTABLE";
        rs = stmt.executeQuery(String.format("SELECT * FROM %s ORDER BY APIARY_EXPORT_TIMESTAMP;", table));
        rs.next();

        // Should be an insert.
        int resExportOp = rs.getInt(3);
        int resKey = rs.getInt(4);
        int resValue = rs.getInt(5);
        assertEquals(ProvenanceBuffer.ExportOperation.INSERT.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(value, resValue);

        // Should be a read.
        rs.next();
        resExportOp = rs.getInt(3);
        resKey = rs.getInt(4);
        resValue = rs.getInt(5);
        assertEquals(ProvenanceBuffer.ExportOperation.READ.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(0, resValue);

        // Should be an update.
        rs.next();
        resExportOp = rs.getInt(3);
        resKey = rs.getInt(4);
        resValue = rs.getInt(5);
        assertEquals(ProvenanceBuffer.ExportOperation.UPDATE.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(value+1, resValue);

        // Should be a read again.
        rs.next();
        resExportOp = rs.getInt(3);
        resKey = rs.getInt(4);
        resValue = rs.getInt(5);
        assertEquals(ProvenanceBuffer.ExportOperation.READ.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(0, resValue);

        // Should be a delete.
        rs.next();
        resExportOp = rs.getInt(3);
        resKey = rs.getInt(4);
        resValue = rs.getInt(5);
        assertEquals(ProvenanceBuffer.ExportOperation.DELETE.getValue(), resExportOp);
        assertEquals(key, resKey);
        assertEquals(value+1, resValue);

        worker.shutdown();
    }
}
