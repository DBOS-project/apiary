package org.dbos.apiary.benchmarks;

import com.google.cloud.storage.*;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ProfileBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(ProfileBenchmark.class);
    private static final int threadPoolSize = 256;

    private static final int initialProfiles = 100;

    private static final int threadWarmupMs = 5000;  // First 5 seconds of request would be warm-up requests.
    private static final Collection<Long> writeTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> readTimes = new ConcurrentLinkedQueue<>();

    public static void benchmark(Integer interval, Integer duration, int percentageRead, int percentageNew, int percentageUpdate) throws SQLException, InterruptedException, IOException {
        assert (percentageRead + percentageNew + percentageUpdate == 100);
        PostgresConnection pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");
        pconn.dropTable("FuncInvocations");
        pconn.dropTable("VersionTable");
        pconn.dropTable("ProfileTable");
        pconn.createTable("VersionTable", "Name varchar(1000), BeginVersion bigint NOT NULL, EndVersion bigint NOT NULL");
        pconn.createTable("ProfileTable", "UserID integer PRIMARY KEY NOT NULL, Name varchar(1000) NOT NULL, Status varchar(2000) NOT NULL");
        pconn.createIndex("CREATE INDEX VersionIndex ON VersionTable (Name, BeginVersion, EndVersion);");

        long tDelete = System.currentTimeMillis();
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Bucket bucket = storage.get(ApiaryConfig.gcsTestBucket);
        List<BlobId> blobIDs = new ArrayList<>();
        for (Blob blob : bucket.list().iterateAll()) {
            blobIDs.add(blob.getBlobId());
        }
        storage.delete(blobIDs);
        logger.info("Cleanup done: {}ms", System.currentTimeMillis() - tDelete);

        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient("localhost"));
        AtomicInteger profileIDs = new AtomicInteger(0);

        long tLoad = System.currentTimeMillis();
        for (int i = 0; i < initialProfiles; i++) {
            int profileID = profileIDs.getAndIncrement();
            String name = "stanford" + profileID;
            String status = "sparking" + profileID;
            String image = String.format("src/test/resources/stanford%d.jpg", profileID % 2);
            client.get().executeFunction("PostgresProfileUpdate", profileID, name, status, image).getInt();
        }
        logger.info("Loading done: {}ms", System.currentTimeMillis() - tLoad);

        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (duration * 1000 + threadWarmupMs);

        Runnable r = () -> {
            try {
                long t0 = System.nanoTime();
                int chooser = ThreadLocalRandom.current().nextInt(100);
                if (chooser < percentageRead) {
                    int profileID = ThreadLocalRandom.current().nextInt(profileIDs.get());
                    client.get().executeFunction("PostgresProfileRead", profileID);
                    readTimes.add(System.nanoTime() - t0);
                } else if (chooser < percentageRead + percentageNew) {
                    int profileID = profileIDs.getAndIncrement();
                    String name = "stanford" + profileID;
                    String status = "sparking" + profileID;
                    String image = String.format("src/test/resources/stanford%d.jpg", profileID % 2);
                    client.get().executeFunction("PostgresProfileUpdate", profileID, name, status, image).getInt();
                    writeTimes.add(System.nanoTime() - t0);
                } else if (chooser < percentageRead + percentageNew + percentageUpdate) {
                    int profileID = ThreadLocalRandom.current().nextInt(profileIDs.get());
                    int newID = ThreadLocalRandom.current().nextInt(profileIDs.get());
                    String name = "stanford" + profileID;
                    String status = "sparking" + newID;
                    String image = String.format("src/test/resources/stanford%d.jpg", newID % 2);
                    client.get().executeFunction("PostgresProfileUpdate", profileID, name, status, image).getInt();
                    writeTimes.add(System.nanoTime() - t0);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        while (System.currentTimeMillis() < endTime) {
            long t = System.nanoTime();
            if (System.currentTimeMillis() - startTime < threadWarmupMs) {
                readTimes.clear();
                writeTimes.clear();
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
            logger.info("Reads: Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
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
            logger.info("Writes: Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No writes");
        }

        threadPool.shutdown();
        threadPool.awaitTermination(100000, TimeUnit.SECONDS);
        logger.info("All queries finished! {}", System.currentTimeMillis() - startTime);
    }
}
