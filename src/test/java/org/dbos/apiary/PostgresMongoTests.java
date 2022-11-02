package org.dbos.apiary;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.client.model.Indexes;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.mongo.MongoConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.mongo.*;
import org.dbos.apiary.procedures.mongo.hotel.MongoAddHotel;
import org.dbos.apiary.procedures.mongo.hotel.MongoMakeReservation;
import org.dbos.apiary.procedures.mongo.hotel.MongoSearchHotel;
import org.dbos.apiary.procedures.postgres.hotel.PostgresAddHotel;
import org.dbos.apiary.procedures.postgres.hotel.PostgresMakeReservation;
import org.dbos.apiary.procedures.postgres.hotel.PostgresSearchHotel;
import org.dbos.apiary.procedures.postgres.pgmongo.*;
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

public class PostgresMongoTests {
    private static final Logger logger = LoggerFactory.getLogger(PostgresMongoTests.class);

    private ApiaryWorker apiaryWorker;

    @BeforeAll
    public static void testConnection() {
        assumeTrue(TestUtils.testMongoConnection());
        assumeTrue(TestUtils.testPostgresConnection());
    }

    @BeforeEach
    public void resetTables() {
        ApiaryConfig.isolationLevel = ApiaryConfig.REPEATABLE_READ;
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");
            conn.dropTable("FuncInvocations");
            conn.dropTable("PersonTable");
            conn.createTable("PersonTable", "Name varchar(1000) PRIMARY KEY NOT NULL, Number integer NOT NULL");
            conn.dropTable("HotelsTable");
            conn.createTable("HotelsTable", "HotelID integer PRIMARY KEY NOT NULL, HotelName VARCHAR(1000) NOT NULL, AvailableRooms integer NOT NULL");
        } catch (Exception e) {
            logger.info("Failed to connect to Postgres.");
        }
        apiaryWorker = null;
    }

    @AfterEach
    public void cleanupWorker() {
        ApiaryConfig.isolationLevel = ApiaryConfig.REPEATABLE_READ;
        if (apiaryWorker != null) {
            apiaryWorker.shutdown();
        }
    }

    @BeforeEach
    public void cleanupMongo() {
        try {
            MongoConnection conn = new MongoConnection("localhost", ApiaryConfig.mongoPort);
            conn.database.getCollection("people").drop();
            conn.database.getCollection("hotels").drop();
            conn.database.getCollection("reservations").drop();
        } catch (Exception e) {
            logger.info("No Mongo/Postgres instance! {}", e.getMessage());
        }
    }

    @Test
    public void testMongoBasic() throws InvalidProtocolBufferException, SQLException {
        logger.info("testMongoBasic");

        MongoConnection conn = new MongoConnection("localhost", ApiaryConfig.mongoPort);
        PostgresConnection pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

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

        res = client.executeFunction("MongoFindPerson", "matei").getInt();
        assertEquals(1, res);
    }

    @Test
    public void testMongoWriteRead() throws InvalidProtocolBufferException, SQLException {
        logger.info("testMongoWriteRead");

        MongoConnection conn = new MongoConnection("localhost", ApiaryConfig.mongoPort);
        PostgresConnection pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.mongo, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresWriteReadPerson", ApiaryConfig.postgres, PostgresWriteReadPerson::new);
        apiaryWorker.registerFunction("MongoWriteReadPerson", ApiaryConfig.mongo, MongoWriteReadPerson::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresWriteReadPerson", "matei", 1).getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresWriteReadPerson", "matei2", 2).getInt();
        assertEquals(1, res);
    }

    @Test
    public void testMongoBulk() throws InvalidProtocolBufferException, SQLException {
        logger.info("testMongoBulk");

        MongoConnection conn = new MongoConnection("localhost", ApiaryConfig.mongoPort);
        PostgresConnection pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.mongo, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresAddPerson", ApiaryConfig.postgres, PostgresAddPerson::new);
        apiaryWorker.registerFunction("PostgresBulkAddPerson", ApiaryConfig.postgres, PostgresBulkAddPerson::new);
        apiaryWorker.registerFunction("PostgresFindPerson", ApiaryConfig.postgres, PostgresFindPerson::new);
        apiaryWorker.registerFunction("MongoAddPerson", ApiaryConfig.mongo, MongoAddPerson::new);
        apiaryWorker.registerFunction("MongoBulkAddPerson", ApiaryConfig.mongo, MongoBulkAddPerson::new);
        apiaryWorker.registerFunction("MongoFindPerson", ApiaryConfig.mongo, MongoFindPerson::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        client.executeFunction("PostgresBulkAddPerson", new String[]{"matei", "christos"}, new int[]{1, 2});

        int res;
        res = client.executeFunction("PostgresFindPerson", "matei").getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresFindPerson", "christos").getInt();
        assertEquals(1, res);
    }

    @Test
    public void testMongoUpdate() throws InvalidProtocolBufferException, SQLException {
        logger.info("testMongoUpdate");

        MongoConnection conn = new MongoConnection("localhost", ApiaryConfig.mongoPort);
        PostgresConnection pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.mongo, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresAddPerson", ApiaryConfig.postgres, PostgresAddPerson::new);
        apiaryWorker.registerFunction("PostgresReplacePerson", ApiaryConfig.postgres, PostgresReplacePerson::new);
        apiaryWorker.registerFunction("PostgresFindPerson", ApiaryConfig.postgres, PostgresFindPerson::new);
        apiaryWorker.registerFunction("MongoAddPerson", ApiaryConfig.mongo, MongoAddPerson::new);
        apiaryWorker.registerFunction("MongoReplacePerson", ApiaryConfig.mongo, MongoReplacePerson::new);
        apiaryWorker.registerFunction("MongoFindPerson", ApiaryConfig.mongo, MongoFindPerson::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresAddPerson", "matei", 1).getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresFindPerson", "matei").getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresReplacePerson", "matei", 2).getInt();
        assertEquals(2, res);

        res = client.executeFunction("PostgresFindPerson", "matei").getInt();
        assertEquals(1, res);
    }

    @Test
    public void testMongoConcurrent() throws InterruptedException, SQLException {
        logger.info("testMongoConcurrent");

        MongoConnection conn = new MongoConnection("localhost", ApiaryConfig.mongoPort);
        PostgresConnection pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

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

    @Test
    public void testMongoConcurrentUpdates() throws InterruptedException, InvalidProtocolBufferException, SQLException {
        logger.info("testMongoConcurrentUpdates");

        MongoConnection conn = new MongoConnection("localhost", ApiaryConfig.mongoPort);
        PostgresConnection pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        int numThreads = 10;
        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numThreads);
        apiaryWorker.registerConnection(ApiaryConfig.mongo, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresAddPerson", ApiaryConfig.postgres, PostgresAddPerson::new);
        apiaryWorker.registerFunction("PostgresReplacePerson", ApiaryConfig.postgres, PostgresReplacePerson::new);
        apiaryWorker.registerFunction("PostgresFindPerson", ApiaryConfig.postgres, PostgresFindPerson::new);
        apiaryWorker.registerFunction("MongoAddPerson", ApiaryConfig.mongo, MongoAddPerson::new);
        apiaryWorker.registerFunction("MongoReplacePerson", ApiaryConfig.mongo, MongoReplacePerson::new);
        apiaryWorker.registerFunction("MongoFindPerson", ApiaryConfig.mongo, MongoFindPerson::new);
        apiaryWorker.startServing();

        long start = System.currentTimeMillis();
        long testDurationMs = 5000L;
        int maxTag = 10;
        AtomicInteger count = new AtomicInteger(0);
        AtomicBoolean success = new AtomicBoolean(true);
        ApiaryWorkerClient c = new ApiaryWorkerClient("localhost");
        for (int i = 0; i < maxTag; i++) {
            c.executeFunction("PostgresAddPerson", "matei" + i, i).getInt();
        }
        Runnable r = () -> {
            try {
                ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");
                while (System.currentTimeMillis() < start + testDurationMs) {
                    int localTag = ThreadLocalRandom.current().nextInt(maxTag);
                    int localCount = count.getAndIncrement();
                    client.executeFunction("PostgresReplacePerson", "matei" + localTag, localCount).getInt();
                    String search = "matei" + localTag;
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

    @Test
    public void testMongoHotel() throws InvalidProtocolBufferException, SQLException {
        logger.info("testMongoHotel");

        MongoConnection conn = new MongoConnection("localhost", ApiaryConfig.mongoPort);
        PostgresConnection pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.mongo, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresAddHotel", ApiaryConfig.postgres, PostgresAddHotel::new);
        apiaryWorker.registerFunction("PostgresMakeReservation", ApiaryConfig.postgres, PostgresMakeReservation::new);
        apiaryWorker.registerFunction("PostgresSearchHotel", ApiaryConfig.postgres, PostgresSearchHotel::new);
        apiaryWorker.registerFunction("MongoMakeReservation", ApiaryConfig.mongo, MongoMakeReservation::new);
        apiaryWorker.registerFunction("MongoAddHotel", ApiaryConfig.mongo, MongoAddHotel::new);
        apiaryWorker.registerFunction("MongoSearchHotel", ApiaryConfig.mongo, MongoSearchHotel::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresAddHotel", 0, "hotel0", 1, 5, 5).getInt();
        assertEquals(0, res);
        res = client.executeFunction("PostgresAddHotel", 1, "hotel1", 10, 10, 10).getInt();
        assertEquals(1, res);

        conn.database.getCollection("hotels").createIndex(Indexes.geo2dsphere("point"));

        int[] resArray;
        resArray = client.executeFunction("PostgresSearchHotel", 6, 6).getIntArray();
        assertEquals(0, resArray[0]);

        resArray = client.executeFunction("PostgresSearchHotel", 11, 11).getIntArray();
        assertEquals(1, resArray[0]);

        res = client.executeFunction("PostgresMakeReservation", 0, 0, 0).getInt();
        assertEquals(0, res);
        res = client.executeFunction("PostgresMakeReservation", 1, 0, 0).getInt();
        assertEquals(1, res);
        res = client.executeFunction("PostgresMakeReservation", 2, 1, 1).getInt();
        assertEquals(0, res);
    }
}
