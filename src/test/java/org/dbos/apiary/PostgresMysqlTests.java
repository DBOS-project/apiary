package org.dbos.apiary;


import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.mysql.MysqlConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.mysql.MysqlQueryPerson;
import org.dbos.apiary.procedures.mysql.MysqlUpsertPerson;
import org.dbos.apiary.procedures.postgres.pgmysql.PostgresQueryPerson;
import org.dbos.apiary.procedures.postgres.pgmysql.PostgresUpsertPerson;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PostgresMysqlTests {
    private static final Logger logger = LoggerFactory.getLogger(PostgresMysqlTests.class);

    private ApiaryWorker apiaryWorker;
    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, ApiaryConfig.postgres, "dbos");
            conn.dropTable("FuncInvocations");
            conn.dropTable("PersonTable");
            conn.createTable("PersonTable", "Name varchar(1000) PRIMARY KEY NOT NULL, Number integer NOT NULL");
        } catch (Exception e) {
            logger.info("Failed to connect to Postgres.");
        }

        try {
            MysqlConnection conn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");
            conn.dropTable("PersonTable");
            // TODO: need to solve the primary key issue. Currently cannot have primary keys.
            conn.createTable("PersonTable", "Name varchar(1000) NOT NULL, Number integer NOT NULL");
        } catch (Exception e) {
            logger.info("Failed to connect to MySQL.");
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
    public void testMysqlBasic() throws InvalidProtocolBufferException {
        logger.info("testMysqlBasic");

        MysqlConnection conn;
        PostgresConnection pconn;
        try {
            conn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, ApiaryConfig.postgres, "dbos");
        } catch (Exception e) {
            logger.info("No MySQL/Postgres instance! {}", e.getMessage());
            return;
        }

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
    public void testMysqlUpdate() throws InvalidProtocolBufferException {
        logger.info("testMysqlUpdate");

        MysqlConnection conn;
        PostgresConnection pconn;
        try {
            conn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, ApiaryConfig.postgres, "dbos");
        } catch (Exception e) {
            logger.info("No MySQL/Postgres instance! {}", e.getMessage());
            return;
        }

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

        res = client.executeFunction("PostgresUpsertPerson", "matei", 2).getInt();
        assertEquals(2, res);

        res = client.executeFunction("PostgresQueryPerson", "matei").getInt();
        assertEquals(1, res);
    }

    @Test
    public void testMysqlConcurrentInsert() throws InterruptedException {
        logger.info("testMysqlConcurrentInsert");

        MysqlConnection conn;
        PostgresConnection pconn;
        try {
            conn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, ApiaryConfig.postgres, "dbos");
        } catch (Exception e) {
            logger.info("No MySQL/Postgres instance! {}", e.getMessage());
            return;
        }

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
    public void testMysqlConcurrentUpdates() throws InterruptedException {
        logger.info("testMysqlConcurrentUpdates");

        MysqlConnection conn;
        PostgresConnection pconn;
        try {
            conn = new MysqlConnection("localhost", ApiaryConfig.mysqlPort, "dbos", "root", "dbos");
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, ApiaryConfig.postgres, "dbos");
        } catch (Exception e) {
            logger.info("No MySQL/Postgres instance! {}", e.getMessage());
            return;
        }

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
                    String search = "matei" + localTag;
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
