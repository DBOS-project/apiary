package org.dbos.apiary;

import org.dbos.apiary.procedures.cockroachdb.CockroachDBFibSumFunction;
import org.dbos.apiary.procedures.cockroachdb.CockroachDBFibonacciFunction;
import org.dbos.apiary.cockroachdb.CockroachDBConnection;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.worker.ApiaryWorkerClient;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

import org.postgresql.ds.PGSimpleDataSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CockroachDBTests {
    private static final Logger logger = LoggerFactory.getLogger(CockroachDBTests.class);

    @Test
    public void testFibCockroachDB() throws Exception {
        logger.info("testFibCockroachDB");

        try {
            PGSimpleDataSource ds = new PGSimpleDataSource();
            ds.setServerNames(new String[] { "localhost" });
            ds.setPortNumbers(new int[] { 26257 });
            ds.setDatabaseName("test");
            ds.setUser("root");
            ds.setSsl(false);

            CockroachDBConnection c = new CockroachDBConnection(ds, /* tableName= */"KVTable");

            c.dropAndCreateTable(/* tableName= */"KVTable",
                    /* columnSpecStr= */"(KVKey integer PRIMARY KEY NOT NULL, KVValue integer NOT NULL)");

            c.registerFunction("FibonacciFunction", () -> {
                return new CockroachDBFibonacciFunction(c.getConnectionForFunction());
            });
            c.registerFunction("FibSumFunction", () -> {
                return new CockroachDBFibSumFunction(c.getConnectionForFunction());
            });
            ApiaryWorker worker = new ApiaryWorker(c, new ApiaryNaiveScheduler(), 128);
            worker.startServing();

            ZContext clientContext = new ZContext();
            ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);

            String res;
            res = client.executeFunction("localhost", "FibonacciFunction", "defaultService", "1");
            assertEquals("1", res);

            res = client.executeFunction("localhost", "FibonacciFunction", "defaultService", "6");
            assertEquals("8", res);

            res = client.executeFunction("localhost", "FibonacciFunction", "defaultService", "10");
            assertEquals("55", res);

            clientContext.close();
            worker.shutdown();
        } catch (PSQLException e) {
            logger.info("No CockroachDB cluster!");
        }

    }
}
