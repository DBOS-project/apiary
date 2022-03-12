package org.dbos.apiary.benchmarks;

import com.google.protobuf.InvalidProtocolBufferException;

import org.dbos.apiary.cockroachdb.CockroachDBConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryWorkerClient;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.ProcCallException;
import org.zeromq.ZContext;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class CockroachDBIncrementBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(IncrementBenchmark.class);

    private static final int threadWarmupMs = 5000; // First 5 seconds of request would be warm-up requests.
    private static final int threadPoolSize = 256;

    public static void benchmark(String cockroachAddr, Integer interval, Integer duration)
            throws IOException, InterruptedException, ProcCallException, SQLException {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[] { cockroachAddr });
        ds.setPortNumbers(new int[] { ApiaryConfig.cockroachdbPort });
        ds.setDatabaseName("test");
        ds.setUser("root");
        ds.setSsl(false);

        CockroachDBConnection ctxt = new CockroachDBConnection(ds, "KVTable");
        ctxt.deleteEntriesFromTable(/* tableName= */"KVTable");

        ZContext clientContext = new ZContext();
        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal
                .withInitial(() -> new ApiaryWorkerClient(ZContext.shadow(clientContext)));

        Collection<Long> trialTimes = new ConcurrentLinkedQueue<>();

        final int numKeys = 100000;
        Runnable r = () -> {
            long rStart = System.nanoTime();
            try {
                String key = String.valueOf(ThreadLocalRandom.current().nextInt(numKeys));
                client.get().executeFunction(ctxt.getHostname(new Object[] { key }), "IncrementFunction", key);
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
            trialTimes.add(System.nanoTime() - rStart);
        };

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (duration * 1000 + threadWarmupMs);

        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);
        while (System.currentTimeMillis() < endTime) {
            long t = System.nanoTime();
            if ((System.currentTimeMillis() - startTime) <= threadWarmupMs) {
                // Clean up the arrays if still in warm up time.
                trialTimes.clear();
            }
            threadPool.submit(r);
            while (System.nanoTime() - t < interval.longValue() * 1000) {
                // Busy-spin
            }
        }
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long elapsedTime = (System.currentTimeMillis() - startTime) - threadWarmupMs;
        List<Long> queryTimes = trialTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        int numQueries = queryTimes.size();
        long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
        double throughput = (double) numQueries * 1000.0 / elapsedTime;
        long p50 = queryTimes.get(numQueries / 2);
        long p99 = queryTimes.get((numQueries * 99) / 100);
        logger.info("Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime,
                interval, numQueries, String.format("%.03f", throughput), average, p50, p99);

        threadPool.shutdown();
        threadPool.awaitTermination(100000, TimeUnit.SECONDS);
        logger.info("All queries finished! {}ms", System.currentTimeMillis() - startTime);
    }
}
