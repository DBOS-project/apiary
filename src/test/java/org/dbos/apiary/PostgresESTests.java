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
import org.dbos.apiary.procedures.postgres.pges.PostgresBulkIndexPerson;
import org.dbos.apiary.procedures.postgres.pges.PostgresIndexPerson;
import org.dbos.apiary.procedures.postgres.pges.PostgresSearchPerson;
import org.dbos.apiary.procedures.postgres.shop.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class PostgresESTests {
    private static final Logger logger = LoggerFactory.getLogger(PostgresESTests.class);

    private ApiaryWorker apiaryWorker;

    @BeforeAll
    public static void testConnection() {
        assumeTrue(TestUtils.testPostgresConnection());
        assumeTrue(TestUtils.testESConnection());
    }

    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");
            conn.dropTable("FuncInvocations");
            conn.dropTable("PersonTable");
            conn.dropTable("ShopItems");
            conn.dropTable("ShopCart");
            conn.dropTable("ShopOrders");
            conn.dropTable("ShopTransactions");
            conn.createTable("PersonTable", "Name varchar(1000) PRIMARY KEY NOT NULL, Number integer NOT NULL");
            conn.createTable("ShopItems", "ItemID integer PRIMARY KEY NOT NULL, ItemName varchar(1000) NOT NULL, ItemDesc varchar(2000) NOT NULL, Cost integer NOT NULL, Inventory integer NOT NULL");
            conn.createTable("ShopCart", "PersonID integer NOT NULL, ItemID integer NOT NULL, Cost integer NOT NULL");
            conn.createTable("ShopOrders", "PersonID integer NOT NULL, OrderID integer NOT NULL, ItemID integer NOT NULL");
            conn.createTable("ShopTransactions", "OrderID integer PRIMARY KEY NOT NULL, PersonID integer NOT NULL, Cost integer NOT NULL");
            conn.createIndex("CREATE INDEX CartIndex ON ShopCart (PersonID);");
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
            request = new DeleteIndexRequest.Builder().index("people").ignoreUnavailable(true).build();
            client.indices().delete(request);
            request = new DeleteIndexRequest.Builder().index("items").ignoreUnavailable(true).build();
            client.indices().delete(request);
        } catch (Exception e) {
            logger.info("Index Not Deleted {}", e.getMessage());
            assumeTrue(false);
        }
    }

    @Test
    public void testElasticsearchBasic() throws InvalidProtocolBufferException, InterruptedException {
        logger.info("testElasticsearchBasic");

        ElasticsearchConnection conn;
        PostgresConnection pconn;
        try {
            conn = new ElasticsearchConnection("localhost", 9200, "elastic", "password");
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");
        } catch (Exception e) {
            logger.info("No Elasticsearch/Postgres instance! {}", e.getMessage());
            return;
        }

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.elasticsearch, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresSearchPerson", ApiaryConfig.postgres, PostgresSearchPerson::new);
        apiaryWorker.registerFunction("PostgresIndexPerson", ApiaryConfig.postgres, PostgresIndexPerson::new);
        apiaryWorker.registerFunction("ElasticsearchIndexPerson", ApiaryConfig.elasticsearch, ElasticsearchIndexPerson::new);
        apiaryWorker.registerFunction("ElasticsearchSearchPerson", ApiaryConfig.elasticsearch, ElasticsearchSearchPerson::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresIndexPerson", "matei", 1).getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresSearchPerson", "matei").getInt();
        assertEquals(1, res);
    }

    @Test
    public void testElasticsearchBulk() throws InvalidProtocolBufferException, InterruptedException {
        logger.info("testElasticsearchBulk");

        ElasticsearchConnection conn;
        PostgresConnection pconn;
        try {
            conn = new ElasticsearchConnection("localhost", 9200, "elastic", "password");
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");
        } catch (Exception e) {
            logger.info("No Elasticsearch/Postgres instance! {}", e.getMessage());
            return;
        }

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.elasticsearch, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresSearchPerson", ApiaryConfig.postgres, PostgresSearchPerson::new);
        apiaryWorker.registerFunction("PostgresBulkIndexPerson", ApiaryConfig.postgres, PostgresBulkIndexPerson::new);
        apiaryWorker.registerFunction("ElasticsearchBulkIndexPerson", ApiaryConfig.elasticsearch, ElasticsearchBulkIndexPerson::new);
        apiaryWorker.registerFunction("ElasticsearchSearchPerson", ApiaryConfig.elasticsearch, ElasticsearchSearchPerson::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int numItems = 10;
        String[] names = new String[numItems];
        int[] numbers = new int[numItems];
        for (int i = 0 ; i < numItems; i++) {
            names[i] = "matei" + i;
            numbers[i] = i;
        }

        int res;
        res = client.executeFunction("PostgresBulkIndexPerson", names, numbers).getInt();
        assertEquals(0, res);

        for (int i = 0 ; i < numItems; i++) {
            res = client.executeFunction("PostgresSearchPerson", "matei" + i).getInt();
            assertEquals(1, res);
        }
    }

    @Test
    public void testElasticsearchUpdate() throws InvalidProtocolBufferException, InterruptedException {
        logger.info("testElasticsearchUpdate");

        ElasticsearchConnection conn;
        PostgresConnection pconn;
        try {
            conn = new ElasticsearchConnection("localhost", 9200, "elastic", "password");
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");
        } catch (Exception e) {
            logger.info("No Elasticsearch/Postgres instance! {}", e.getMessage());
            return;
        }

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.elasticsearch, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresSearchPerson", ApiaryConfig.postgres, PostgresSearchPerson::new);
        apiaryWorker.registerFunction("PostgresIndexPerson", ApiaryConfig.postgres, PostgresIndexPerson::new);
        apiaryWorker.registerFunction("ElasticsearchIndexPerson", ApiaryConfig.elasticsearch, ElasticsearchIndexPerson::new);
        apiaryWorker.registerFunction("ElasticsearchSearchPerson", ApiaryConfig.elasticsearch, ElasticsearchSearchPerson::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("PostgresIndexPerson", "matei", 1).getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresSearchPerson", "matei").getInt();
        assertEquals(1, res);

        res = client.executeFunction("PostgresIndexPerson", "matei", 2).getInt();
        assertEquals(2, res);

        res = client.executeFunction("PostgresSearchPerson", "matei").getInt();
        assertEquals(1, res);
    }

    @Test
    public void testElasticsearchConcurrent() throws InterruptedException {
        logger.info("testElasticsearchConcurrent");

        ElasticsearchConnection conn;
        PostgresConnection pconn;
        try {
            conn = new ElasticsearchConnection("localhost", 9200, "elastic", "password");
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");
        } catch (Exception e) {
            logger.info("No Elasticsearch/Postgres instance! {}", e.getMessage());
            return;
        }

        int numThreads = 10;
        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numThreads);
        apiaryWorker.registerConnection(ApiaryConfig.elasticsearch, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresSearchPerson", ApiaryConfig.postgres, PostgresSearchPerson::new);
        apiaryWorker.registerFunction("PostgresIndexPerson", ApiaryConfig.postgres, PostgresIndexPerson::new);
        apiaryWorker.registerFunction("ElasticsearchIndexPerson", ApiaryConfig.elasticsearch, ElasticsearchIndexPerson::new);
        apiaryWorker.registerFunction("ElasticsearchSearchPerson", ApiaryConfig.elasticsearch, ElasticsearchSearchPerson::new);
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
                    client.executeFunction("PostgresIndexPerson", "matei" + localCount, localCount).getInt();
                    String search = "matei" + ThreadLocalRandom.current().nextInt(localCount - 5, localCount + 5);
                    int res = client.executeFunction("PostgresSearchPerson", search).getInt();
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
    public void testElasticsearchConcurrentUpdates() throws InterruptedException {
        logger.info("testElasticsearchConcurrentUpdates");

        ElasticsearchConnection conn;
        PostgresConnection pconn;
        try {
            conn = new ElasticsearchConnection("localhost", 9200, "elastic", "password");
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");
        } catch (Exception e) {
            logger.info("No Elasticsearch/Postgres instance! {}", e.getMessage());
            return;
        }

        int numThreads = 10;
        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numThreads);
        apiaryWorker.registerConnection(ApiaryConfig.elasticsearch, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresSearchPerson", ApiaryConfig.postgres, PostgresSearchPerson::new);
        apiaryWorker.registerFunction("PostgresIndexPerson", ApiaryConfig.postgres, PostgresIndexPerson::new);
        apiaryWorker.registerFunction("ElasticsearchIndexPerson", ApiaryConfig.elasticsearch, ElasticsearchIndexPerson::new);
        apiaryWorker.registerFunction("ElasticsearchSearchPerson", ApiaryConfig.elasticsearch, ElasticsearchSearchPerson::new);
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
                    client.executeFunction("PostgresIndexPerson", "matei" + localTag, localCount).getInt();
                    String search = "matei" + localTag;
                    int res = client.executeFunction("PostgresSearchPerson", search).getInt();
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
    public void testShopBenchmark() throws InvalidProtocolBufferException {
        logger.info("testShopBenchmark");

        ElasticsearchConnection conn;
        PostgresConnection pconn;
        try {
            conn = new ElasticsearchConnection("localhost", 9200, "elastic", "password");
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");
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
        apiaryWorker.registerFunction("ShopGetItem", ApiaryConfig.postgres, ShopGetItem::new);
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
        res = client.executeFunction("ShopGetItem", 0, "camera", 10).getInt();
        assertEquals(0, res);
        res = client.executeFunction("ShopCheckoutCart", 0).getInt();
        assertEquals(14, res);
    }
}
