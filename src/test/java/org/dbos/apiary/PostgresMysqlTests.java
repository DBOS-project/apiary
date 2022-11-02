package org.dbos.apiary;


import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.mysql.MysqlConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.mysql.*;
import org.dbos.apiary.procedures.postgres.pgmysql.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class PostgresMysqlTests {
    private static final Logger logger = LoggerFactory.getLogger(PostgresMysqlTests.class);

    private ApiaryWorker apiaryWorker;

    public PostgresMysqlTests() throws SQLException {
    }

    @BeforeAll
    public static void testConnection() {
        assumeTrue(TestUtils.testMysqlConnection());
        assumeTrue(TestUtils.testPostgresConnection());
    }


    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos");
            conn.dropTable("FuncInvocations");
            conn.dropTable("PersonTable");
            conn.createTable("PersonTable", "Name varchar(1000) PRIMARY KEY NOT NULL, Number integer NOT NULL");
        } catch (Exception e) {
            logger.info("Failed to connect to Postgres.");
            assumeTrue(false);
        }

        try {
            MysqlConnection conn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");
            conn.dropTable("PersonTable");
            if (ApiaryConfig.XDBTransactions) {
                // TODO: need to solve the primary key issue. Currently cannot have primary keys.
                conn.createTable("PersonTable", "Name varchar(100) NOT NULL, Number integer NOT NULL, PRIMARY KEY (__apiaryID__, __beginVersion__), " +
                        "  KEY(__endVersion__), " +  "  KEY(Name)");
            } else {
                conn.createTable("PersonTable", "Name varchar(100) PRIMARY KEY NOT NULL, Number integer NOT NULL");
            }
        } catch (Exception e) {
            logger.info("Failed to connect to MySQL.");
            assumeTrue(false);
        }

        apiaryWorker = null;
    }

    @AfterEach
    public void cleanupWorker() {
        if (apiaryWorker != null) {
            apiaryWorker.shutdown();
        }
    }

    @Test
    public void testMysqlBasic() throws SQLException, InvalidProtocolBufferException {
        logger.info("testMysqlBasic");

        MysqlConnection conn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");

        PostgresConnection pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.mysql, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresUpsertPerson", ApiaryConfig.postgres, PostgresUpsertPerson::new);
        apiaryWorker.registerFunction("PostgresQueryPerson", ApiaryConfig.postgres, PostgresQueryPerson::new);
        apiaryWorker.registerFunction("MysqlUpsertPerson", ApiaryConfig.mysql, MysqlUpsertPerson::new);
        apiaryWorker.registerFunction("MysqlQueryPerson", ApiaryConfig.mysql, MysqlQueryPerson::new);

        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresUpsertPerson", "matei", 1).getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresQueryPerson", "matei").getInt();
        assertEquals(1, res);
    }

    @Test
    public void testMysqlUpdate() throws InvalidProtocolBufferException, SQLException {
        logger.info("testMysqlUpdate");
        MysqlConnection conn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");

        PostgresConnection pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.mysql, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresUpsertPerson", ApiaryConfig.postgres, PostgresUpsertPerson::new);
        apiaryWorker.registerFunction("PostgresQueryPerson", ApiaryConfig.postgres, PostgresQueryPerson::new);
        apiaryWorker.registerFunction("PostgresReplacePerson", ApiaryConfig.postgres, PostgresMysqlReplacePerson::new);
        apiaryWorker.registerFunction("MysqlUpsertPerson", ApiaryConfig.mysql, MysqlUpsertPerson::new);
        apiaryWorker.registerFunction("MysqlReplacePerson", ApiaryConfig.mysql, MysqlReplacePerson::new);
        apiaryWorker.registerFunction("MysqlQueryPerson", ApiaryConfig.mysql, MysqlQueryPerson::new);

        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresUpsertPerson", "matei", 1).getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresQueryPerson", "matei").getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresReplacePerson", "matei", 2).getInt();
        assertEquals(2, res);

        res = client.executeFunction("PostgresQueryPerson", "matei").getInt();
        assertEquals(1, res);
    }

    @Test
    public void testMysqlBulk() throws InvalidProtocolBufferException, SQLException {
        logger.info("testMysqlBulk");
        MysqlConnection conn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");

        PostgresConnection pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.mysql, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresQueryPerson", ApiaryConfig.postgres, PostgresQueryPerson::new);
        apiaryWorker.registerFunction("PostgresMysqlBulkAddPerson", ApiaryConfig.postgres, PostgresMysqlBulkAddPerson::new);
        apiaryWorker.registerFunction("MysqlBulkAddPerson", ApiaryConfig.mysql, MysqlBulkAddPerson::new);
        apiaryWorker.registerFunction("MysqlQueryPerson", ApiaryConfig.mysql, MysqlQueryPerson::new);

        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresMysqlBulkAddPerson",  new String[]{"matei", "christos"}, new int[]{1, 2}).getInt();
        assertEquals(2, res);

        res = client.executeFunction("PostgresQueryPerson", "christos").getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresQueryPerson", "matei").getInt();
        assertEquals(1, res);
    }

    @Test
    public void testMysqlWriteRead() throws SQLException, InvalidProtocolBufferException {
        logger.info("testMysqlWriteRead");

        MysqlConnection conn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");

        PostgresConnection pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.mysql, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresMysqlWriteReadPerson", ApiaryConfig.postgres, PostgresMysqlWriteReadPerson::new);
        apiaryWorker.registerFunction("MysqlWriteReadPerson", ApiaryConfig.mysql, MysqlWriteReadPerson::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresMysqlWriteReadPerson", "matei", 1).getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresMysqlWriteReadPerson", "matei2", 2).getInt();
        assertEquals(2, res);
    }

    @Test
    public void testMysqlConcurrentInsert() throws InterruptedException, SQLException {
        // Only test if it's using XDBT.
        assumeTrue(ApiaryConfig.XDBTransactions);
        logger.info("testMysqlConcurrentInsert");

        MysqlConnection conn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");

        PostgresConnection pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos");

        int numThreads = 10;
        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numThreads);
        apiaryWorker.registerConnection(ApiaryConfig.mysql, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresUpsertPerson", ApiaryConfig.postgres, PostgresUpsertPerson::new);
        apiaryWorker.registerFunction("PostgresQueryPerson", ApiaryConfig.postgres, PostgresQueryPerson::new);
        apiaryWorker.registerFunction("MysqlUpsertPerson", ApiaryConfig.mysql, MysqlUpsertPerson::new);
        apiaryWorker.registerFunction("MysqlQueryPerson", ApiaryConfig.mysql, MysqlQueryPerson::new);

        apiaryWorker.startServing();


        long start = System.currentTimeMillis();
        long testDurationMs = 5000L;
        AtomicInteger count = new AtomicInteger(0);
        AtomicBoolean success = new AtomicBoolean(true);
        Runnable r = () -> {
            try {
                ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");
                while (System.currentTimeMillis() < start + testDurationMs) {
                    int localCount = count.getAndIncrement();
                    client.executeFunction("PostgresUpsertPerson", "matei" + localCount, localCount).getInt();
                    String search = "matei" + ThreadLocalRandom.current().nextInt(localCount - 5, localCount + 5);
                    int res = client.executeFunction("PostgresQueryPerson", search).getInt();
                    if (res == -1) {
                        success.set(false);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                success.set(false);
            }
        };

        List<Thread> threads = new ArrayList<>();
        for (int threadNum = 0; threadNum < numThreads; threadNum++) {
            Thread t = new Thread(r);
            threads.add(t);
            t.start();
        }
        for (Thread t: threads) {
            t.join();
        }
        assertTrue(success.get());
    }

    @Test
    public void testMysqlConcurrentUpdates() throws InterruptedException, SQLException {
        // Only test if it's using XDBT.
        assumeTrue(ApiaryConfig.XDBTransactions);
        logger.info("testMysqlConcurrentUpdates");

        MysqlConnection conn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");

        PostgresConnection pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos");

        int numThreads = 10;
        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numThreads);
        apiaryWorker.registerConnection(ApiaryConfig.mysql, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresUpsertPerson", ApiaryConfig.postgres, PostgresUpsertPerson::new);
        apiaryWorker.registerFunction("PostgresQueryPerson", ApiaryConfig.postgres, PostgresQueryPerson::new);
        apiaryWorker.registerFunction("MysqlUpsertPerson", ApiaryConfig.mysql, MysqlUpsertPerson::new);
        apiaryWorker.registerFunction("MysqlQueryPerson", ApiaryConfig.mysql, MysqlQueryPerson::new);

        apiaryWorker.startServing();

        long start = System.currentTimeMillis();
        long testDurationMs = 5000L;
        int maxTag = 10;
        AtomicInteger count = new AtomicInteger(0);
        AtomicBoolean success = new AtomicBoolean(true);
        Runnable r = () -> {
            try {
                ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");
                while (System.currentTimeMillis() < start + testDurationMs) {
                    int localTag = ThreadLocalRandom.current().nextInt(maxTag);
                    int localCount = count.getAndIncrement();
                    client.executeFunction("PostgresUpsertPerson", "matei" + localTag, localCount).getInt();
                    String search = "matei" + ThreadLocalRandom.current().nextInt(localTag - 5, localTag + 5);
                    int res = client.executeFunction("PostgresQueryPerson", search).getInt();
                    if (res == -1) {
                        success.set(false);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                success.set(false);
            }
        };

        List<Thread> threads = new ArrayList<>();
        for (int threadNum = 0; threadNum < numThreads; threadNum++) {
            Thread t = new Thread(r);
            threads.add(t);
            t.start();
        }
        for (Thread t: threads) {
            t.join();
        }
        assertTrue(success.get());
    }
}
