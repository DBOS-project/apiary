package org.dbos.apiary;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.interposition.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.PostgresFibSumFunction;
import org.dbos.apiary.procedures.postgres.PostgresFibonacciFunction;
import org.dbos.apiary.procedures.postgres.retwis.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.worker.ApiaryWorkerClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    public void testFibPostgres() throws InvalidProtocolBufferException, InterruptedException {
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

        ZContext clientContext = new ZContext();
        ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);

        int res;
        res = client.executeFunction("localhost", "PostgresFibonacciFunction", "defaultService", 1l, 1).getInt();
        assertEquals(1, res);

        res = client.executeFunction("localhost", "PostgresFibonacciFunction", "defaultService", 2l, 6).getInt();
        assertEquals(8, res);

        res = client.executeFunction("localhost", "PostgresFibonacciFunction", "defaultService", 3l, 10).getInt();
        assertEquals(55, res);

        // Should be able to see provenance data if Vertica is running.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
        clientContext.close();
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

        ZContext clientContext = new ZContext();
        ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);

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

        String resString;
        resString = client.executeFunction("localhost", "RetwisGetPosts", "defaultService", 0).getString();
        assertEquals("hello0,hello1", resString);
        String res;
        res = client.executeFunction("localhost", "RetwisGetFollowees", "defaultService", 1).getString();
        assertEquals(2, res.split(",").length);
        assertTrue(res.contains("0"));
        assertTrue(res.contains("1"));
        res = client.executeFunction("localhost", "RetwisGetTimeline", "defaultService", 1).getString();
        assertEquals(3, res.split(",").length);
        assertTrue(res.contains("hello0"));
        assertTrue(res.contains("hello1"));
        assertTrue(res.contains("hello2"));

        clientContext.close();
        worker.shutdown();

    }
}
