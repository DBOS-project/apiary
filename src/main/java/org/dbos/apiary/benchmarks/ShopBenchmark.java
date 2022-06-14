package org.dbos.apiary.benchmarks;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.elasticsearch.ElasticsearchConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.elasticsearch.shop.ShopItem;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ShopBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(ShopBenchmark.class);
    private static final int threadPoolSize = 256;

    private static final int initialItems = 10;
    private static final int numPeople = 100;

    private static final int threadWarmupMs = 5000;  // First 5 seconds of request would be warm-up requests.
    private static final Collection<Long> writeTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> readTimes = new ConcurrentLinkedQueue<>();

    // Requires file part.tbl in the data/ folder from TPC-H.  Download here: https://kraftp-uniserve-data.s3.us-east-2.amazonaws.com/TPC-H-SF1/part.tbl
    public static void benchmark(String dbAddr, Integer interval, Integer duration, int percentageGetItem, int percentageCheckout, int percentageWrite) throws SQLException, InterruptedException, IOException {
        assert (percentageGetItem + percentageCheckout + percentageWrite == 100);
        PostgresConnection conn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
        conn.dropTable("FuncInvocations");
        conn.dropTable("ShopItems");
        conn.dropTable("ShopCart");
        conn.dropTable("ShopOrders");
        conn.dropTable("ShopTransactions");
        conn.createTable("ShopItems", "ItemID integer PRIMARY KEY NOT NULL, ItemName varchar(1000) NOT NULL, ItemDesc varchar(2000) NOT NULL, Cost integer NOT NULL, Inventory integer NOT NULL");
        conn.createTable("ShopCart", "PersonID integer NOT NULL, ItemID integer NOT NULL, Cost integer NOT NULL");
        conn.createTable("ShopOrders", "PersonID integer NOT NULL, OrderID integer NOT NULL, ItemID integer NOT NULL");
        conn.createTable("ShopTransactions", "OrderID integer PRIMARY KEY NOT NULL, PersonID integer NOT NULL, Cost integer NOT NULL");
        conn.createIndex("CREATE INDEX CartIndex ON ShopCart (PersonID);");
        ElasticsearchClient esClient = new ElasticsearchConnection("localhost", 9200, "elastic", "password").client;
        try {
            DeleteIndexRequest request = new DeleteIndexRequest.Builder().index("items").build();
            esClient.indices().delete(request);
        } catch (Exception e) {
            logger.info("Index Not Deleted {}", e.getMessage());
        }
        esClient.shutdown();

        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient("localhost"));

        long loadStart = System.currentTimeMillis();
        List<ShopItem> partData = partData(Path.of("data", "part.tbl"));
        int[] itemIDs = new int[initialItems];
        String[] itemNames = new String[initialItems];
        String[] itemDescs = new String[initialItems];
        int[] costs = new int[initialItems];
        int[] inventories = new int[initialItems];
        for (int i = 0; i < initialItems; i++) {
            ShopItem item = partData.get(i);
            itemIDs[i] = Integer.parseInt(item.getItemID());
            itemNames[i] = item.getItemName();
            itemDescs[i] = item.getItemDesc();
            costs[i] = item.getCost();
            inventories[i] = 100000000;
        }
        client.get().executeFunction("ShopBulkAddItem", itemIDs, itemNames, itemDescs, costs, inventories);
        logger.info("Done Loading: {}", System.currentTimeMillis() - loadStart);

        AtomicInteger count = new AtomicInteger(initialItems);
        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (duration * 1000 + threadWarmupMs);

        Runnable r = () -> {
            try {
                long t0 = System.nanoTime();
                int chooser = ThreadLocalRandom.current().nextInt(100);
                if (chooser < percentageGetItem) {
                    int personID = ThreadLocalRandom.current().nextInt(numPeople);
                    String search = partData.get(ThreadLocalRandom.current().nextInt(count.get())).getItemName();
                    client.get().executeFunction("ShopGetItem", personID, search, 1150).getInt();
                    readTimes.add(System.nanoTime() - t0);
                } else if (chooser < percentageGetItem + percentageCheckout) {
                    int personID = ThreadLocalRandom.current().nextInt(numPeople);
                    client.get().executeFunction("ShopCheckoutCart", personID).getInt();
                    readTimes.add(System.nanoTime() - t0);
                } else {
                    int localCount = count.incrementAndGet();
                    ShopItem item = partData.get(localCount);
                    client.get().executeFunction("ShopAddItem", localCount, item.getItemName(), item.getItemDesc(), item.getCost(), 100000000);
                    writeTimes.add(System.nanoTime() - t0);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        while (System.currentTimeMillis() < endTime) {
            long t = System.nanoTime();
            if (System.currentTimeMillis() - startTime < threadWarmupMs) {
                writeTimes.clear();
                readTimes.clear();
            }
            threadPool.submit(r);
            while (System.nanoTime() - t < interval.longValue() * 1000) {
                // Busy-spin
            }
        }

        long elapsedTime = (System.currentTimeMillis() - startTime) - threadWarmupMs;

        List<Long> queryTimes = readTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        int numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No reads");
        }

        queryTimes = writeTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No writes");
        }

        threadPool.shutdown();
        threadPool.awaitTermination(100000, TimeUnit.SECONDS);
        logger.info("All queries finished! {}", System.currentTimeMillis() - startTime);
        System.exit(0); // ES client is bugged and won't exit.
    }

    public static List<ShopItem> partData(Path orderPath) throws IOException {
        List<ShopItem> items = new ArrayList<>();
        BufferedReader reader =
                new BufferedReader(new FileReader(orderPath.toFile()), 10000000);
        String line;
        while ((line = reader.readLine()) != null) {
            String[] entries = line.split(Pattern.quote("|"));
            assert(entries.length == 9);
            ShopItem item = new ShopItem();
            item.setItemID(entries[0]);
            item.setItemDesc(entries[8]);
            item.setItemName(entries[1]);
            item.setCost((int) Double.parseDouble(entries[7]));
            items.add(item);
        }
        return items;
    }

}
