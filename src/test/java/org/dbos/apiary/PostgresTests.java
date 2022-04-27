package org.dbos.apiary;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.PostgresFibSumFunction;
import org.dbos.apiary.procedures.postgres.PostgresFibonacciFunction;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.worker.ApiaryWorkerClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PostgresTests {
    private static final Logger logger = LoggerFactory.getLogger(PostgresTests.class);

    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection ctxt = new PostgresConnection("localhost", ApiaryConfig.postgresPort);
            ctxt.createTable("KVTable", "(KVKey integer PRIMARY KEY NOT NULL, KVValue integer NOT NULL)");
            ctxt.truncateTable("KVTable");
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

        ZContext clientContext = new ZContext();
        ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);

        String res;
        res = client.executeFunction("localhost", "PostgresFibonacciFunction", "defaultService", 1).getString();
        assertEquals("1", res);

        res = client.executeFunction("localhost", "PostgresFibonacciFunction", "defaultService", 6).getString();
        assertEquals("8", res);

        res = client.executeFunction("localhost", "PostgresFibonacciFunction", "defaultService", 10).getString();
        assertEquals("55", res);

        clientContext.close();
        worker.shutdown();

    }
}
