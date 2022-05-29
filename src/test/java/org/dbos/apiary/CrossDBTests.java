package org.dbos.apiary;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.elasticsearch.ElasticsearchConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.elasticsearch.ElasticsearchIndexPerson;
import org.dbos.apiary.procedures.elasticsearch.ElasticsearchSearchPerson;
import org.dbos.apiary.procedures.postgres.tests.PostgresIndexPerson;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CrossDBTests {
    private static final Logger logger = LoggerFactory.getLogger(CrossDBTests.class);

    private ApiaryWorker apiaryWorker;

    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
            conn.dropTable("RecordedOutputs");
            conn.dropTable("FuncInvocations");
            conn.dropTable("PersonTable");
            conn.createTable("PersonTable", "Name varchar(1000) NOT NULL, Number integer PRIMARY KEY NOT NULL");
        } catch (Exception e) {
            e.printStackTrace();
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
    public void cleanupElasticsearch() {
        try {
            ElasticsearchClient client = new ElasticsearchConnection("localhost", 9200, "elastic", "password").client;
            DeleteIndexRequest request = new DeleteIndexRequest.Builder().index("people").build();
            client.indices().delete(request);
        } catch (Exception e) {
            logger.info("Index Not Deleted {}", e.getMessage());
        }
    }

    @Test
    public void testElasticsearchBasic() throws InvalidProtocolBufferException, InterruptedException {
        logger.info("testElasticsearchBasic");

        ElasticsearchConnection conn;
        PostgresConnection pconn;
        try {
            conn = new ElasticsearchConnection("localhost", 9200, "elastic", "password");
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
        } catch (Exception e) {
            logger.info("No Elasticsearch/Postgres instance! {}", e.getMessage());
            return;
        }

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.elasticsearch, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresIndexPerson", ApiaryConfig.postgres, PostgresIndexPerson::new);
        apiaryWorker.registerFunction("ElasticsearchIndexPerson", ApiaryConfig.elasticsearch, ElasticsearchIndexPerson::new);
        apiaryWorker.registerFunction("ElasticsearchSearchPerson", ApiaryConfig.elasticsearch, ElasticsearchSearchPerson::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresIndexPerson", "matei", 1).getInt();
        assertEquals(1, res);

        Thread.sleep(5000);

        res = client.executeFunction("ElasticsearchSearchPerson", "matei").getInt();
        assertEquals(1, res);
    }
}
