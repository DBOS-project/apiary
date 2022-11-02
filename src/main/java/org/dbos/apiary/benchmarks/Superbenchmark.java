package org.dbos.apiary.benchmarks;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.client.model.Indexes;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.elasticsearch.ElasticsearchConnection;
import org.dbos.apiary.mongo.MongoConnection;
import org.dbos.apiary.mongo.MongoContext;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Superbenchmark {
    private static final Logger logger = LoggerFactory.getLogger(Superbenchmark.class);
    private static final int threadPoolSize = 256;

    private static final int initialDocs = 1000000;
    private static final int chunkSize = 100000;

    private static final int threadWarmupMs = 5000;  // First 5 seconds of request would be warm-up requests.
    private static final Collection<Long> writeTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> readTimes = new ConcurrentLinkedQueue<>();

    public static void benchmark(String dbAddr, Integer interval, Integer duration, int percentageRead, int percentageUpdate) throws SQLException, InterruptedException, InvalidProtocolBufferException {
        assert(percentageRead + percentageUpdate == 100);

        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");
            conn.dropTable("FuncInvocations");
            conn.dropTable("SuperbenchmarkTable");
            conn.createTable("SuperbenchmarkTable", "ItemID integer PRIMARY KEY NOT NULL, Inventory integer NOT NULL");
        } catch (Exception e) {
            logger.info("Failed to connect to Postgres.");
        }
        try {
            ElasticsearchClient client = new ElasticsearchConnection(dbAddr, 9200, "elastic", "password").client;
            DeleteIndexRequest request;
            request = new DeleteIndexRequest.Builder().index("superbenchmark").ignoreUnavailable(true).build();
            client.indices().delete(request);
        } catch (Exception e) {
            logger.info("Index Not Deleted {}", e.getMessage());
        }
        MongoConnection mconn;
        try {
            mconn = new MongoConnection(dbAddr, ApiaryConfig.mongoPort);
            mconn.database.getCollection("superbenchmark").drop();
        } catch (Exception e) {
            logger.info("No Mongo instance! {}", e.getMessage());
            return;
        }

        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient("localhost"));
        AtomicInteger count = new AtomicInteger(0);

        long loadStart = System.currentTimeMillis();
        assert (initialDocs % chunkSize == 0);
        int numChunks = initialDocs / chunkSize;
        List<String> names = new ArrayList<>();
        for (int chunkNum = 0; chunkNum < numChunks; chunkNum++) {
            int[] initialIDs = new int[chunkSize];
            String[] initialNames = new String[chunkSize];
            int[] initialCosts = new int[chunkSize];
            int[] initialInventories = new int[chunkSize];
            for (int i = 0; i < chunkSize; i++) {
                int num = count.getAndIncrement();
                initialIDs[i] = num;
                String name = generateRandomWord(10);
                names.add(name);
                initialNames[i] = name;
                initialCosts[i] = ThreadLocalRandom.current().nextInt(100);
                initialInventories[i] = ThreadLocalRandom.current().nextInt(10000);
            }
            client.get().executeFunction("PostgresSBBulkWrite", initialIDs, initialNames, initialCosts, initialInventories);
        }
        logger.info("Done Loading: {}", System.currentTimeMillis() - loadStart);

        if (ApiaryConfig.XDBTransactions) {
            mconn.database.getCollection("superbenchmark").createIndex(Indexes.ascending(MongoContext.beginVersion));
            mconn.database.getCollection("superbenchmark").createIndex(Indexes.ascending(MongoContext.endVersion));
        }
        mconn.database.getCollection("superbenchmark").createIndex(Indexes.ascending(MongoContext.apiaryID));
        mconn.database.getCollection("superbenchmark").createIndex(Indexes.ascending("itemID"));

        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (duration * 1000 + threadWarmupMs);

        Runnable r = () -> {
            try {
                int chooser = ThreadLocalRandom.current().nextInt(100);
                if (chooser < percentageRead) {
                    long t0 = System.nanoTime();
                    String search = names.get(ThreadLocalRandom.current().nextInt(names.size()));
                    client.get().executeFunction("PostgresSBRead", search);
                    readTimes.add(System.nanoTime() - t0);
                } else if (chooser < percentageRead + percentageUpdate) {
                    long t0 = System.nanoTime();
                    int itemID = ThreadLocalRandom.current().nextInt(initialDocs);
                    String newName = generateRandomWord(10);
                    client.get().executeFunction("PostgresSBUpdate", itemID, newName,
                            ThreadLocalRandom.current().nextInt(100), ThreadLocalRandom.current().nextInt(10000));
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

    private static String generateRandomWord(int length) {
        char[] word = new char[length];
        for(int j = 0; j < word.length; j++)
        {
            word[j] = (char)('a' + ThreadLocalRandom.current().nextInt(26));
        }
        return new String(word);
    }
}
