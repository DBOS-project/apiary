package org.dbos.apiary;

import org.dbos.apiary.executor.Executor;
import org.dbos.apiary.procedures.sqlite.SQLiteFibSumFunction;
import org.dbos.apiary.procedures.sqlite.SQLiteFibonacciFunction;
import org.dbos.apiary.sqlite.SQLiteConnection;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.dbos.apiary.utilities.ApiaryConfig.defaultPkey;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SQLiteTests {
    private static final Logger logger = LoggerFactory.getLogger(SQLiteTests.class);

    @Test
    public void testFibSQLite() throws Exception {
        logger.info("testFibSQLite");
        Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:");

        SQLiteConnection c = new SQLiteConnection(conn);
        c.createTable("CREATE TABLE KVTable(pkey integer NOT NULL, KVKey integer NOT NULL, KVValue integer NOT NULL);");
        c.registerFunction("FibonacciFunction", new SQLiteFibonacciFunction(conn));
        c.registerFunction("FibSumFunction", new SQLiteFibSumFunction(conn));

        String res = Executor.executeFunction(c, "FibonacciFunction", defaultPkey, "1");
        assertEquals("1", res);
        res = Executor.executeFunction(c, "FibonacciFunction", defaultPkey, "3");
        assertEquals("2", res);
        res = Executor.executeFunction(c, "FibonacciFunction", defaultPkey, "4");
        assertEquals("3", res);
        res = Executor.executeFunction(c, "FibonacciFunction", defaultPkey, "10");
        assertEquals("55", res);
        res = Executor.executeFunction(c, "FibonacciFunction", defaultPkey, "30");
        assertEquals("832040", res);

        conn.close();
    }
}
