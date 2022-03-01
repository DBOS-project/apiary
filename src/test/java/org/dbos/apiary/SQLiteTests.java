package org.dbos.apiary;

import org.dbos.apiary.procedures.sqlite.SQLiteFibSumFunction;
import org.dbos.apiary.procedures.sqlite.SQLiteFibonacciFunction;
import org.dbos.apiary.sqlite.SQLiteConnection;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.worker.ApiaryWorkerClient;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SQLiteTests {
    private static final Logger logger = LoggerFactory.getLogger(SQLiteTests.class);

    @Test
    public void testFibSQLite() throws Exception {
        logger.info("testFibSQLite");

        // TODO: need to set numWorkerThreads to 1. Because SQLite doesn't support concurrent transactions from two threads right now. Maybe use a separate SQLite connection per worker thread?

        ApiaryWorker.numWorkerThreads = 1;
        Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:");

        SQLiteConnection c = new SQLiteConnection(conn);
        c.createTable("CREATE TABLE KVTable(KVKey integer NOT NULL, KVValue integer NOT NULL);");
        c.registerFunction("FibonacciFunction", () -> new SQLiteFibonacciFunction(conn));
        c.registerFunction("FibSumFunction", () -> new SQLiteFibSumFunction(conn));
        ApiaryWorker worker = new ApiaryWorker(c);
        worker.startServing();

        ZContext clientContext = new ZContext();
        ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);

        String res;
        res = client.executeFunction("localhost", "FibonacciFunction", "1");
        assertEquals("1", res);

        res = client.executeFunction("localhost", "FibonacciFunction", "10");
        assertEquals("55", res);

        res = client.executeFunction("localhost", "FibonacciFunction", "30");
        assertEquals("832040", res);

        clientContext.close();
        worker.shutdown();
        conn.close();
    }
}
