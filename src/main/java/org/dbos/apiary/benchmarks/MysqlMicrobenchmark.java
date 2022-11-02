package org.dbos.apiary.benchmarks;

import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.mysql.MysqlConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.mysql.MysqlBulkAddPerson;
import org.dbos.apiary.procedures.mysql.MysqlQueryPerson;
import org.dbos.apiary.procedures.mysql.MysqlReplacePerson;
import org.dbos.apiary.procedures.mysql.MysqlUpsertPerson;
import org.dbos.apiary.procedures.postgres.pgmysql.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MysqlMicrobenchmark {
    private static final Logger logger = LoggerFactory.getLogger(MysqlMicrobenchmark.class);

    private static final int threadPoolSize = 128;
    private static final int numWorker = 16;

    private static final int numPeople = 1000000;
    private static final int chunkSize = 1000;

    private static final int threadWarmupMs = 5000;  // First 5 seconds of request would be warm-up requests.
    private static final Collection<Long> readTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> insertTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> updateTimes = new ConcurrentLinkedQueue<>();

    public static void benchmark(String dbAddr, Integer interval, Integer duration, int percentageRead, int percentageAppend, int percentageUpdate) throws SQLException, InterruptedException, IOException {
        assert (percentageRead + percentageAppend + percentageUpdate == 100);
        ApiaryConfig.captureUpdates = false;
        ApiaryConfig.captureReads = false;

        PostgresConnection pgConn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "dbos");
        pgConn.dropTable("FuncInvocations");

        MysqlConnection mysqlConn = new MysqlConnection(dbAddr, ApiaryConfig.mysqlPort, "dbos", "root", "dbos");

        // Start serving.
        ApiaryWorker apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numWorker);
        apiaryWorker.registerConnection(ApiaryConfig.mysql, mysqlConn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pgConn);
        apiaryWorker.registerFunction("PostgresMysqlSoloAddPerson", ApiaryConfig.postgres, PostgresMysqlSoloAddPerson::new);
        apiaryWorker.registerFunction("PostgresMysqlSoloBulkAddPerson", ApiaryConfig.postgres, PostgresMysqlSoloBulkAddPerson::new);
        apiaryWorker.registerFunction("PostgresMysqlSoloQueryPerson", ApiaryConfig.postgres, PostgresMysqlSoloQueryPerson::new);
        apiaryWorker.registerFunction("PostgresMysqlSoloReplacePerson", ApiaryConfig.postgres, PostgresMysqlSoloReplacePerson::new);

        apiaryWorker.registerFunction("MysqlBulkAddPerson", ApiaryConfig.mysql, MysqlBulkAddPerson::new);
        apiaryWorker.registerFunction("MysqlQueryPerson", ApiaryConfig.mysql, MysqlQueryPerson::new);
        apiaryWorker.registerFunction("MysqlReplacePerson", ApiaryConfig.mysql, MysqlReplacePerson::new);
        apiaryWorker.registerFunction("MysqlUpsertPerson", ApiaryConfig.mysql, MysqlUpsertPerson::new);


        apiaryWorker.startServing();

        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient("localhost"));

        if (BenchmarkingExecutable.skipLoadData) {
            logger.info("Skip loading data, {} people", numPeople);
        } else {
            mysqlConn.dropTable("PersonTable");
            if (ApiaryConfig.XDBTransactions) {
                // TODO: need to solve the primary key issue. Currently cannot have primary keys.
                mysqlConn.createTable("PersonTable", "Name varchar(100) NOT NULL, Number integer NOT NULL, PRIMARY KEY (Name, __beginVersion__), " +
                        "  KEY(__endVersion__), KEY (__apiaryID__), KEY(__beginVersion__) ");
            } else {
                mysqlConn.createTable("PersonTable", "Name varchar(100) PRIMARY KEY NOT NULL, Number integer NOT NULL");
            }

            long loadStart = System.currentTimeMillis();

            int numChunks = numPeople / chunkSize;
            int count = 0;
            for (int chunkNum = 0; chunkNum < numChunks; chunkNum++) {
                String[] initialNames = new String[chunkSize];
                int[] initialNumbers = new int[chunkSize];
                for (int i = 0; i < chunkSize; i++) {
                    int num = count;
                    String name = "matei" + num;
                    initialNames[i] = name;
                    initialNumbers[i] = num;
                    count++;
                }
                client.get().executeFunction("PostgresMysqlSoloBulkAddPerson", initialNames, initialNumbers);
            }

            int res = client.get().executeFunction("PostgresMysqlSoloQueryPerson", "matei" + (numPeople - 1)).getInt();
            assert (res == 0);
            logger.info("Done loading {} people: {}ms", numPeople, System.currentTimeMillis() - loadStart);
        }

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
                    client.get().executeFunction("MysqlQueryPerson", "matei" + personNum);
                    readTimes.add(System.nanoTime() - t0);
                } else if (chooser < percentageRead + percentageAppend) {
                    int personID = personNums.getAndIncrement();
                    if (ApiaryConfig.XDBTransactions) {
                        client.get().executeFunction("PostgresMysqlSoloAddPerson", "matei" + personID, personID);
                    } else {
                        client.get().executeFunction("MysqlUpsertPerson", "matei" + personID, personID);
                    }
                    insertTimes.add(System.nanoTime() - t0);
                } else {
                    int personID = ThreadLocalRandom.current().nextInt(personNums.get() - 100);
                    int num = ThreadLocalRandom.current().nextInt();
                    if (ApiaryConfig.XDBTransactions) {
                        client.get().executeFunction("PostgresMysqlSoloReplacePerson", "matei" + personID, num);
                    } else {
                        client.get().executeFunction("MysqlReplacePerson", "matei" + personID, num);
                    }
                    updateTimes.add(System.nanoTime() - t0);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        while (System.currentTimeMillis() < endTime) {
            long t = System.nanoTime();
            if (System.currentTimeMillis() - startTime < threadWarmupMs) {
                readTimes.clear();
                insertTimes.clear();
                updateTimes.clear();
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

        queryTimes = insertTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("Inserts: Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No inserts");
        }

        queryTimes = updateTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("Updates: Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No updates");
        }

        threadPool.shutdown();
        threadPool.awaitTermination(100000, TimeUnit.SECONDS);

        apiaryWorker.shutdown();
        logger.info("All queries finished! {}", System.currentTimeMillis() - startTime);
        System.exit(0);
    }
}
