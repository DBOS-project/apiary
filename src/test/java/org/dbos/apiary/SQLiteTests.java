package org.dbos.apiary;

import org.dbos.apiary.procedures.sqlite.SQLiteFibSumFunction;
import org.dbos.apiary.procedures.sqlite.SQLiteFibonacciFunction;
import org.dbos.apiary.sqlite.SQLiteConnection;
import org.dbos.apiary.sqlite.SQLitePartitionInfo;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.worker.ApiaryWorkerClient;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SQLiteTests {
    private static final Logger logger = LoggerFactory.getLogger(SQLiteTests.class);

    @Test
    public void testFibSQLite() throws Exception {
        logger.info("testFibSQLite");
        Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:");

        SQLiteConnection c = new SQLiteConnection(conn);
        c.createTable("CREATE TABLE KVTable(pkey integer NOT NULL, KVKey integer NOT NULL, KVValue integer NOT NULL);");
        c.registerFunction("FibonacciFunction", () -> new SQLiteFibonacciFunction(conn));
        c.registerFunction("FibSumFunction", () -> new SQLiteFibSumFunction(conn));
        SQLitePartitionInfo partitionInfo = new SQLitePartitionInfo();
        ApiaryWorker worker = new ApiaryWorker(c, partitionInfo);
        worker.startServing();

        ZContext clientContext = new ZContext();
        ApiaryWorkerClient client = new ApiaryWorkerClient(clientContext);

        String res;
        res = client.executeFunction("localhost:8000", "FibonacciFunction", ApiaryConfig.defaultPkey, "1");
        assertEquals("1", res);

        res = client.executeFunction("localhost:8000", "FibonacciFunction", ApiaryConfig.defaultPkey, "10");
        assertEquals("55", res);

        res = client.executeFunction("localhost:8000", "FibonacciFunction", ApiaryConfig.defaultPkey, "30");
        assertEquals("832040", res);

        clientContext.close();
        worker.shutdown();
        conn.close();
    }
}
