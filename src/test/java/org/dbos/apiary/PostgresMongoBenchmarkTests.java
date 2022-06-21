package org.dbos.apiary;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.client.model.Indexes;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.mongo.MongoConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.mongo.hotel.MongoAddHotel;
import org.dbos.apiary.procedures.mongo.hotel.MongoMakeReservation;
import org.dbos.apiary.procedures.mongo.hotel.MongoSearchHotel;
import org.dbos.apiary.procedures.postgres.hotel.PostgresAddHotel;
import org.dbos.apiary.procedures.postgres.hotel.PostgresMakeReservation;
import org.dbos.apiary.procedures.postgres.hotel.PostgresSearchHotel;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PostgresMongoBenchmarkTests {
    private static final Logger logger = LoggerFactory.getLogger(PostgresESTests.class);

    private ApiaryWorker apiaryWorker;

    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
            conn.dropTable("FuncInvocations");
            conn.dropTable("HotelsTable");
            conn.createTable("HotelsTable", "HotelID integer PRIMARY KEY NOT NULL, HotelName VARCHAR(1000) NOT NULL, AvailableRooms integer NOT NULL");
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
    public void testMongoHotel() throws InvalidProtocolBufferException {
        logger.info("testMongoHotel");

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

        res = client.executeFunction("PostgresSearchHotel", 6, 6).getInt();
        assertEquals(0, res);

        res = client.executeFunction("PostgresSearchHotel", 11, 11).getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresMakeReservation", 0, 0, 0).getInt();
        assertEquals(0, res);
        res = client.executeFunction("PostgresMakeReservation", 1, 0, 0).getInt();
        assertEquals(1, res);
        res = client.executeFunction("PostgresMakeReservation", 2, 1, 1).getInt();
        assertEquals(0, res);
    }
}
