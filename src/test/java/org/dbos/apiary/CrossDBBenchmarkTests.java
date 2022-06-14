package org.dbos.apiary;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.elasticsearch.ElasticsearchConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.elasticsearch.ElasticsearchBulkIndexPerson;
import org.dbos.apiary.procedures.elasticsearch.ElasticsearchIndexPerson;
import org.dbos.apiary.procedures.elasticsearch.ElasticsearchSearchPerson;
import org.dbos.apiary.procedures.elasticsearch.shop.ShopESAddItem;
import org.dbos.apiary.procedures.elasticsearch.shop.ShopESSearchItem;
import org.dbos.apiary.procedures.postgres.crossdb.PostgresBulkIndexPerson;
import org.dbos.apiary.procedures.postgres.crossdb.PostgresIndexPerson;
import org.dbos.apiary.procedures.postgres.crossdb.PostgresSearchPerson;
import org.dbos.apiary.procedures.postgres.shop.ShopAddCart;
import org.dbos.apiary.procedures.postgres.shop.ShopAddItem;
import org.dbos.apiary.procedures.postgres.shop.ShopCheckoutCart;
import org.dbos.apiary.procedures.postgres.shop.ShopSearchItem;
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

public class CrossDBBenchmarkTests {
    private static final Logger logger = LoggerFactory.getLogger(CrossDBBenchmarkTests.class);

    private ApiaryWorker apiaryWorker;

    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
            conn.dropTable("FuncInvocations");
            conn.dropTable("ShopItems");
            conn.dropTable("ShopCart");
            conn.dropTable("ShopOrders");
            conn.dropTable("ShopTransactions");
            conn.createTable("ShopItems", "ItemID integer PRIMARY KEY NOT NULL, ItemName varchar(1000) NOT NULL, ItemDesc varchar(2000) NOT NULL, Cost integer NOT NULL, Inventory integer NOT NULL");
            conn.createTable("ShopCart", "PersonID integer NOT NULL, ItemID integer NOT NULL, Cost integer NOT NULL");
            conn.createTable("ShopOrders", "PersonID integer NOT NULL, OrderID integer NOT NULL, ItemID integer NOT NULL");
            conn.createTable("ShopTransactions", "OrderID integer PRIMARY KEY NOT NULL, PersonID integer NOT NULL, Cost integer NOT NULL");
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
    public void cleanupElasticsearch() {
        try {
            ElasticsearchClient client = new ElasticsearchConnection("localhost", 9200, "elastic", "password").client;
            DeleteIndexRequest request = new DeleteIndexRequest.Builder().index("items").build();
            client.indices().delete(request);
        } catch (Exception e) {
            logger.info("Index Not Deleted {}", e.getMessage());
        }
    }

    @Test
    public void testShopBenchmark() throws InvalidProtocolBufferException {
        logger.info("testShopBenchmark");

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
        apiaryWorker.registerFunction("ShopAddItem", ApiaryConfig.postgres, ShopAddItem::new);
        apiaryWorker.registerFunction("ShopSearchItem", ApiaryConfig.postgres, ShopSearchItem::new);
        apiaryWorker.registerFunction("ShopAddCart", ApiaryConfig.postgres, ShopAddCart::new);
        apiaryWorker.registerFunction("ShopCheckoutCart", ApiaryConfig.postgres, ShopCheckoutCart::new);
        apiaryWorker.registerFunction("ShopESAddItem", ApiaryConfig.elasticsearch, ShopESAddItem::new);
        apiaryWorker.registerFunction("ShopESSearchItem", ApiaryConfig.elasticsearch, ShopESSearchItem::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");
        int res;
        String[] items;
        res = client.executeFunction("ShopAddItem", 0, "camera", "good camera", 5, 5).getInt();
        res = client.executeFunction("ShopAddItem", 1, "stove", "good stove", 4, 1).getInt();
        items = client.executeFunction("ShopSearchItem", "camera", 10).getStringArray();
        assertEquals(1, items.length);
        assertEquals("0", items[0]);
        items = client.executeFunction("ShopSearchItem", "camera", 3).getStringArray();
        assertEquals(0, items.length);
        items = client.executeFunction("ShopSearchItem", "stove", 10).getStringArray();
        assertEquals(1, items.length);
        assertEquals("1", items[0]);
        items = client.executeFunction("ShopSearchItem", "nothing", 10).getStringArray();
        assertEquals(0, items.length);
        res = client.executeFunction("ShopAddCart", 0, 0).getInt();
        assertEquals(0, res);
        res = client.executeFunction("ShopAddCart", 0, 1).getInt();
        assertEquals(0, res);
        res = client.executeFunction("ShopAddCart", 0, 1).getInt();
        assertEquals(1, res);
        res = client.executeFunction("ShopCheckoutCart", 0).getInt();
        assertEquals(9, res);
    }
}
