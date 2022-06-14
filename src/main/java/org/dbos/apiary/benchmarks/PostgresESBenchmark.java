package org.dbos.apiary.benchmarks;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.elasticsearch.ElasticsearchConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PostgresESBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(PostgresESBenchmark.class);
    private static final int threadPoolSize = 256;
    private static final int percentageRead = 99;
    private static final int percentageAppend = 1;
    private static final int percentageUpdate = 0;

    private static final int initialDocs = 100000;

    private static final int threadWarmupMs = 5000;  // First 5 seconds of request would be warm-up requests.
    private static final Collection<Long> writeTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> readTimes = new ConcurrentLinkedQueue<>();

    public static void benchmark(String dbAddr, String service, Integer interval, Integer duration) throws SQLException, InterruptedException, InvalidProtocolBufferException {
        assert(percentageRead + percentageAppend + percentageUpdate == 100);

        PostgresConnection conn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
        conn.dropTable("FuncInvocations");
        conn.dropTable("PersonTable");
        conn.createTable("PersonTable", "Name varchar(1000) PRIMARY KEY NOT NULL, Number integer NOT NULL");
        ElasticsearchClient esClient = new ElasticsearchConnection("localhost", 9200, "elastic", "password").client;
        try {
            DeleteIndexRequest request = new DeleteIndexRequest.Builder().index("people").build();
            esClient.indices().delete(request);
        } catch (Exception e) {
            logger.info("Index Not Deleted {}", e.getMessage());
        }
        esClient.shutdown();

        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient("localhost"));

        long loadStart = System.currentTimeMillis();
        String[] initialNames = new String[initialDocs];
        int[] initialNumbers = new int[initialDocs];
        for (int i = 0; i < initialDocs; i++) {
            initialNames[i] = "matei" + i;
            initialNumbers[i] = i;
        }
        client.get().executeFunction("PostgresBulkIndexPerson", initialNames, initialNumbers);
        logger.info("Done Loading: {}", System.currentTimeMillis() - loadStart);

        AtomicInteger count = new AtomicInteger(initialDocs);
        ExecutorService readThreadPool = Executors.newFixedThreadPool(threadPoolSize);
        ExecutorService writeThreadPool = Executors.newFixedThreadPool(threadPoolSize);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (duration * 1000 + threadWarmupMs);

        Runnable readR = () -> {
            try {
                long t0 = System.nanoTime();
                String search = "matei" + ThreadLocalRandom.current().nextInt(count.get() - 5, count.get() + 5);
                int res = client.get().executeFunction("PostgresSearchPerson", search).getInt();
                if (ApiaryConfig.XDBTransactions && res == -1) {
                    logger.info("Inconsistency: {}", search);
                }
                readTimes.add(System.nanoTime() - t0);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        Runnable appendR = () -> {
            try {
                long t0 = System.nanoTime();
                int localCount = count.getAndIncrement();
                client.get().executeFunction("PostgresIndexPerson", "matei" + localCount, localCount).getInt();
                writeTimes.add(System.nanoTime() - t0);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        Runnable updateR = () -> {
            try {
                long t0 = System.nanoTime();
                int localCount = ThreadLocalRandom.current().nextInt(count.get());
                client.get().executeFunction("PostgresIndexPerson", "matei" + localCount, localCount).getInt();
                writeTimes.add(System.nanoTime() - t0);
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
            int chooser = ThreadLocalRandom.current().nextInt(100);
            if (chooser < percentageRead) {
                readThreadPool.submit(readR);
            } else if (chooser < percentageRead + percentageAppend) {
                writeThreadPool.submit(appendR);
            } else {
                writeThreadPool.submit(updateR);
            }
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

        readThreadPool.shutdown();
        readThreadPool.awaitTermination(100000, TimeUnit.SECONDS);
        writeThreadPool.shutdown();
        writeThreadPool.awaitTermination(100000, TimeUnit.SECONDS);
        logger.info("All queries finished! {}", System.currentTimeMillis() - startTime);
        System.exit(0); // ES client is bugged and won't exit.
    }
}