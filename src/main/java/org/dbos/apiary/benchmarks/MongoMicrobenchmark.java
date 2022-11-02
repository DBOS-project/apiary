package org.dbos.apiary.benchmarks;

import com.mongodb.client.model.Indexes;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.mongo.MongoConnection;
import org.dbos.apiary.mongo.MongoContext;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MongoMicrobenchmark {
    private static final Logger logger = LoggerFactory.getLogger(MongoMicrobenchmark.class);
    private static final int threadPoolSize = 256;

    private static final int numPeople = 1000000;

    private static final int threadWarmupMs = 5000;  // First 5 seconds of request would be warm-up requests.
    private static final Collection<Long> readTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> writeTimes = new ConcurrentLinkedQueue<>();

    public static void benchmark(String dbAddr, Integer interval, Integer duration, int percentageRead, int percentageAppend, int percentageUpdate) throws SQLException, InterruptedException, IOException {
        assert (percentageRead + percentageAppend + percentageUpdate == 100);
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");
        conn.dropTable("FuncInvocations");
        conn.dropTable("PersonTable");
        conn.createTable("PersonTable", "Name varchar(1000) PRIMARY KEY NOT NULL, Number integer NOT NULL");

        MongoConnection mconn = new MongoConnection(dbAddr, ApiaryConfig.mongoPort);
        mconn.database.getCollection("people").drop();

        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient("localhost"));

        long loadStart = System.currentTimeMillis();
        String[] names = new String[numPeople];
        int[] nums = new int[numPeople];
        for (int personNum = 0; personNum < numPeople; personNum++) {
            names[personNum] = "matei" + personNum;
            nums[personNum] = personNum;
        }
        client.get().executeFunction("PostgresBulkAddPerson", names, nums);
        client.get().executeFunction("PostgresFindPerson", "matei" + 0);
        logger.info("Done loading {} people: {}ms", numPeople, System.currentTimeMillis() - loadStart);

        if (ApiaryConfig.XDBTransactions) {
            mconn.database.getCollection("people").createIndex(Indexes.ascending(MongoContext.beginVersion));
        }
        mconn.database.getCollection("people").createIndex(Indexes.ascending(MongoContext.apiaryID));
        mconn.database.getCollection("people").createIndex(Indexes.ascending("name"));

        AtomicInteger personNums = new AtomicInteger(numPeople);
        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (duration * 1000 + threadWarmupMs);

        Runnable r = () -> {
            try {
                long t0 = System.nanoTime();
                int chooser = ThreadLocalRandom.current().nextInt(100);
                if (chooser < percentageRead) {
                    int personNum = ThreadLocalRandom.current().nextInt(personNums.get());
                    client.get().executeFunction("MongoFindPerson", "matei" + personNum);
                    readTimes.add(System.nanoTime() - t0);
                } else if (chooser < percentageRead + percentageAppend) {
                    int personID = personNums.getAndIncrement();
                    if (ApiaryConfig.XDBTransactions) {
                        client.get().executeFunction("PostgresSoloAddPerson", "matei" + personID, personID);
                    } else {
                        client.get().executeFunction("MongoAddPerson", "matei" + personID, personID);
                    }
                    writeTimes.add(System.nanoTime() - t0);
                } else {
                    int personID = ThreadLocalRandom.current().nextInt(personNums.get() - 100);
                    int num = ThreadLocalRandom.current().nextInt();
                    if (ApiaryConfig.XDBTransactions) {
                        client.get().executeFunction("PostgresSoloReplacePerson", "matei" + personID, num);
                    } else {
                        client.get().executeFunction("MongoReplacePerson", "matei" + personID, num);
                    }
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
