package org.dbos.apiary;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.client.model.Indexes;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.elasticsearch.ElasticsearchConnection;
import org.dbos.apiary.mongo.MongoConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.elasticsearch.superbenchmark.ElasticsearchSBBulkWrite;
import org.dbos.apiary.procedures.elasticsearch.superbenchmark.ElasticsearchSBRead;
import org.dbos.apiary.procedures.elasticsearch.superbenchmark.ElasticsearchSBWrite;
import org.dbos.apiary.procedures.mongo.superbenchmark.MongoSBBulkWrite;
import org.dbos.apiary.procedures.mongo.superbenchmark.MongoSBRead;
import org.dbos.apiary.procedures.mongo.superbenchmark.MongoSBUpdate;
import org.dbos.apiary.procedures.mongo.superbenchmark.MongoSBWrite;
import org.dbos.apiary.procedures.postgres.superbenchmark.PostgresSBBulkWrite;
import org.dbos.apiary.procedures.postgres.superbenchmark.PostgresSBRead;
import org.dbos.apiary.procedures.postgres.superbenchmark.PostgresSBUpdate;
import org.dbos.apiary.procedures.postgres.superbenchmark.PostgresSBWrite;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class SuperbenchmarkTests {
    private static final Logger logger = LoggerFactory.getLogger(SuperbenchmarkTests.class);

    private ApiaryWorker apiaryWorker;

    @BeforeAll
    public static void testConnection() {
        assumeTrue(TestUtils.testPostgresConnection());
        assumeTrue(TestUtils.testMongoConnection());
        assumeTrue(TestUtils.testESConnection());
    }

    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");
            conn.dropTable("FuncInvocations");
            conn.dropTable("SuperbenchmarkTable");
            conn.createTable("SuperbenchmarkTable", "ItemID integer PRIMARY KEY NOT NULL, Inventory integer NOT NULL");
        } catch (Exception e) {
            logger.info("Failed to connect to Postgres.");
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

    @BeforeEach
    public void cleanupElasticsearch() {
        try {
            ElasticsearchClient client = new ElasticsearchConnection("localhost", 9200, "elastic", "password").client;
            DeleteIndexRequest request;
            request = new DeleteIndexRequest.Builder().index("superbenchmark").ignoreUnavailable(true).build();
            client.indices().delete(request);
        } catch (Exception e) {
            logger.info("Index Not Deleted {}", e.getMessage());
            assumeTrue(false);
        }
    }

    @BeforeEach
    public void cleanupMongo() {
        try {
            MongoConnection conn = new MongoConnection("localhost", ApiaryConfig.mongoPort);
            conn.database.getCollection("superbenchmark").drop();
        } catch (Exception e) {
            logger.info("No Mongo instance! {}", e.getMessage());
            assumeTrue(false);
        }
    }

    @Test
    public void testSuperbenchmark() throws InvalidProtocolBufferException, InterruptedException {
        logger.info("testSuperbenchmark");

        ElasticsearchConnection econn;
        PostgresConnection pconn;
        MongoConnection mconn;
        try {
            econn = new ElasticsearchConnection("localhost", 9200, "elastic", "password");
            mconn = new MongoConnection("localhost", ApiaryConfig.mongoPort);
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");
        } catch (Exception e) {
            logger.info("No Elasticsearch/Postgres instance! {}", e.getMessage());
            return;
        }

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.elasticsearch, econn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerConnection(ApiaryConfig.mongo, mconn);
        apiaryWorker.registerFunction("PostgresSBWrite", ApiaryConfig.postgres, PostgresSBWrite::new);
        apiaryWorker.registerFunction("PostgresSBBulkWrite", ApiaryConfig.postgres, PostgresSBBulkWrite::new);
        apiaryWorker.registerFunction("PostgresSBUpdate", ApiaryConfig.postgres, PostgresSBUpdate::new);
        apiaryWorker.registerFunction("PostgresSBRead", ApiaryConfig.postgres, PostgresSBRead::new);
        apiaryWorker.registerFunction("ElasticsearchSBWrite", ApiaryConfig.elasticsearch, ElasticsearchSBWrite::new);
        apiaryWorker.registerFunction("ElasticsearchSBBulkWrite", ApiaryConfig.elasticsearch, ElasticsearchSBBulkWrite::new);
        apiaryWorker.registerFunction("ElasticsearchSBRead", ApiaryConfig.elasticsearch, ElasticsearchSBRead::new);
        apiaryWorker.registerFunction("MongoSBWrite", ApiaryConfig.mongo, MongoSBWrite::new);
        apiaryWorker.registerFunction("MongoSBBulkWrite", ApiaryConfig.mongo, MongoSBBulkWrite::new);
        apiaryWorker.registerFunction("MongoSBUpdate", ApiaryConfig.mongo, MongoSBUpdate::new);
        apiaryWorker.registerFunction("MongoSBRead", ApiaryConfig.mongo, MongoSBRead::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int resInt;
        resInt = client.executeFunction("PostgresSBWrite", 1, "spark", 2, 3).getInt();
        assertEquals(0, resInt);

        mconn.database.getCollection("superbenchmark").createIndex(Indexes.ascending("itemID"));

        int[] resIntArray;
        resIntArray = client.executeFunction("PostgresSBRead", "spark").getIntArray();
        assertEquals(1, resIntArray[0]);
        assertEquals(3, resIntArray[1]);
        assertEquals(2, resIntArray[2]);

        resInt = client.executeFunction("PostgresSBUpdate", 1, "spark2", 4, 5).getInt();
        assertEquals(0, resInt);

        resIntArray = client.executeFunction("PostgresSBRead", "spark").getIntArray();
        assertEquals(-1, resIntArray[0]);
        assertEquals(-1, resIntArray[1]);
        assertEquals(-1, resIntArray[2]);

        resIntArray = client.executeFunction("PostgresSBRead", "spark2").getIntArray();
        assertEquals(1, resIntArray[0]);
        assertEquals(5, resIntArray[1]);
        assertEquals(4, resIntArray[2]);

        resInt = client.executeFunction("PostgresSBBulkWrite", new int[]{10, 11}, new String[]{"delta", "lake"},
                new int[]{20, 21}, new int[]{30, 31}).getInt();
        assertEquals(0, resInt);

        resIntArray = client.executeFunction("PostgresSBRead", "delta").getIntArray();
        assertEquals(10, resIntArray[0]);
        assertEquals(30, resIntArray[1]);
        assertEquals(20, resIntArray[2]);

        resIntArray = client.executeFunction("PostgresSBRead", "lake").getIntArray();
        assertEquals(11, resIntArray[0]);
        assertEquals(31, resIntArray[1]);
        assertEquals(21, resIntArray[2]);
    }
}
