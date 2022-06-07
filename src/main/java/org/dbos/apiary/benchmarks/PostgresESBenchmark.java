package org.dbos.apiary.benchmarks;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.elasticsearch.ElasticsearchConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PostgresESBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(PostgresESBenchmark.class);
    private static final int threadPoolSize = 256;

    private static final int threadWarmupMs = 5000;  // First 5 seconds of request would be warm-up requests.
    private static final Collection<Long> trialTimes = new ConcurrentLinkedQueue<>();

    public static void benchmark(String dbAddr, String service, Integer interval, Integer duration) throws SQLException, InterruptedException {

        PostgresConnection conn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
        conn.dropTable("RecordedOutputs");
        conn.dropTable("FuncInvocations");
        conn.dropTable("PersonTable");
        conn.createTable("PersonTable", "Name varchar(1000) NOT NULL, Number integer PRIMARY KEY NOT NULL");
        ElasticsearchClient esClient = new ElasticsearchConnection("localhost", 9200, "elastic", "password").client;
        try {
            DeleteIndexRequest request = new DeleteIndexRequest.Builder().index("people").build();
            esClient.indices().delete(request);
        } catch (Exception e) {
            logger.info("Index Not Deleted {}", e.getMessage());
        }
        esClient.shutdown();

        AtomicInteger count = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (duration * 1000 + threadWarmupMs);

        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient("localhost"));

        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);

        Runnable r = () -> {
            try {
                long t0 = System.nanoTime();
                int localCount = count.getAndIncrement();
                client.get().executeFunction("PostgresIndexPerson", "matei" + localCount, localCount).getInt();
                trialTimes.add(System.nanoTime() - t0);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        while (System.currentTimeMillis() < endTime) {
            long t = System.nanoTime();
            if (System.currentTimeMillis() - startTime < threadWarmupMs) {
                trialTimes.clear();
            }
            threadPool.submit(r);
            while (System.nanoTime() - t < interval.longValue() * 1000) {
                // Busy-spin
            }
        }

        long elapsedTime = (System.currentTimeMillis() - startTime) - threadWarmupMs;
        List<Long> queryTimes = trialTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        int numQueries = queryTimes.size();
        long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
        double throughput = (double) numQueries * 1000.0 / elapsedTime;
        long p50 = queryTimes.get(numQueries / 2);
        long p99 = queryTimes.get((numQueries * 99) / 100);
        logger.info("Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        threadPool.shutdown();
        threadPool.awaitTermination(100000, TimeUnit.SECONDS);
        logger.info("All queries finished! {}", System.currentTimeMillis() - startTime);
        System.exit(0); // ES client is bugged and won't exit.
    }
}
