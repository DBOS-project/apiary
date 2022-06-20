package org.dbos.apiary;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.mongo.MongoConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.mongo.MongoAddPerson;
import org.dbos.apiary.procedures.mongo.MongoFindPerson;
import org.dbos.apiary.procedures.postgres.pgmongo.PostgresAddPerson;
import org.dbos.apiary.procedures.postgres.pgmongo.PostgresFindPerson;
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

public class PostgresMongoTests {
    private static final Logger logger = LoggerFactory.getLogger(PostgresESTests.class);

    private ApiaryWorker apiaryWorker;

    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
            conn.dropTable("FuncInvocations");
            conn.dropTable("PersonTable");
            conn.createTable("PersonTable", "Name varchar(1000) PRIMARY KEY NOT NULL, Number integer NOT NULL");
        } catch (Exception e) {
            logger.info("Failed to connect to Postgres.");
        }
        apiaryWorker = null;
    }

    @AfterEach
    public void cleanupWorker() {
        if (apiaryWorker != null) {
            apiaryWorker.shutdown();
        }
    }

    @BeforeEach
    public void cleanupMongo() {
        try {
            MongoConnection conn = new MongoConnection("localhost", 27017);
            conn.database.getCollection("people").drop();
        } catch (Exception e) {
            logger.info("No Mongo/Postgres instance! {}", e.getMessage());
        }
    }


    @Test
    public void testMongoBasic() throws InvalidProtocolBufferException {
        logger.info("testMongoBasic");

        MongoConnection conn;
        PostgresConnection pconn;
        try {
            conn = new MongoConnection("localhost", 27017);
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
        } catch (Exception e) {
            logger.info("No Mongo/Postgres instance! {}", e.getMessage());
            return;
        }

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.mongo, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresAddPerson", ApiaryConfig.postgres, PostgresAddPerson::new);
        apiaryWorker.registerFunction("PostgresFindPerson", ApiaryConfig.postgres, PostgresFindPerson::new);
        apiaryWorker.registerFunction("MongoAddPerson", ApiaryConfig.mongo, MongoAddPerson::new);
        apiaryWorker.registerFunction("MongoFindPerson", ApiaryConfig.mongo, MongoFindPerson::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresAddPerson", "matei", 1).getInt();
        assertEquals(1, res);


        res = client.executeFunction("PostgresFindPerson", "matei").getInt();
        assertEquals(1, res);
    }

    @Test
    public void testMongoConcurrent() throws InterruptedException {
        logger.info("testMongoConcurrent");

        MongoConnection conn;
        PostgresConnection pconn;
        try {
            conn = new MongoConnection("localhost", 27017);
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
        } catch (Exception e) {
            logger.info("No Mongo/Postgres instance! {}", e.getMessage());
            return;
        }

        int numThreads = 10;
        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numThreads);
        apiaryWorker.registerConnection(ApiaryConfig.mongo, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresAddPerson", ApiaryConfig.postgres, PostgresAddPerson::new);
        apiaryWorker.registerFunction("PostgresFindPerson", ApiaryConfig.postgres, PostgresFindPerson::new);
        apiaryWorker.registerFunction("MongoAddPerson", ApiaryConfig.mongo, MongoAddPerson::new);
        apiaryWorker.registerFunction("MongoFindPerson", ApiaryConfig.mongo, MongoFindPerson::new);
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
                    client.executeFunction("PostgresAddPerson", "matei" + localCount, localCount).getInt();
                    String search = "matei" + ThreadLocalRandom.current().nextInt(localCount - 5, localCount + 5);
                    int res = client.executeFunction("PostgresFindPerson", search).getInt();
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
