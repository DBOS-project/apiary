package org.dbos.apiary.benchmarks.tpcc;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.benchmarks.RetroBenchmark;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.moodle.MDLUtil;
import org.dbos.apiary.procedures.postgres.tpcc.NewOrder;
import org.dbos.apiary.procedures.postgres.tpcc.Payment;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class TPCCBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(TPCCBenchmark.class);

    private static final int threadPoolSize = 32;
    private static final int numWorker = 16;
    private static final int threadWarmupMs = 30000;  // First 30 seconds of requests would be warm-up and not recorded.

    private static final int numWarehouses = 24;

    private static ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient("localhost"));

    private static final Collection<Long> paymentTimes = new ConcurrentLinkedQueue<>();
    private static final Collection<Long> newOrderTimes = new ConcurrentLinkedQueue<>();

    private static Random gen = new Random(0);

    public static void benchmark(String dbAddr, Integer interval, Integer duration, int retroMode, long startExecId, long endExecId, List<Integer> percentages) throws SQLException, InvalidProtocolBufferException, InterruptedException {
        ApiaryConfig.isolationLevel = ApiaryConfig.REPEATABLE_READ;
        int paymentPercentage = percentages.get(0);

        boolean hasProv = ApiaryConfig.recordInput ? true : false;  // Enable provenance?

        if (retroMode == ApiaryConfig.ReplayMode.NOT_REPLAY.getValue()) {
            // We assume TPC-C data has been pre-loaded, only reset provenance capture data.
            resetAllTables(dbAddr);
        }

        PostgresConnection pgConn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "dbos", RetroBenchmark.provenanceDB, RetroBenchmark.provenanceAddr);

        ApiaryWorker apiaryWorker;
        if (hasProv) {
            // Enable provenance logging in the worker.
            apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numWorker, RetroBenchmark.provenanceDB, RetroBenchmark.provenanceAddr);
        } else {
            // Disable provenance.
            apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numWorker);
        }
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pgConn);

        apiaryWorker.registerFunction(TPCCUtil.PAYMENT_FUNC, ApiaryConfig.postgres, Payment::new, false);
        apiaryWorker.registerFunction(TPCCUtil.NEWORDER_FUNC, ApiaryConfig.postgres, NewOrder::new, false);

        apiaryWorker.startServing();

        if (retroMode > 0) {
            long startTime = System.currentTimeMillis();
            RetroBenchmark.retroReplayExec(client.get(), retroMode, startExecId, endExecId);
            long elapsedTime = System.currentTimeMillis() - startTime;
            apiaryWorker.shutdown();
            logger.info("Replay mode {}, execution time: {} ms", retroMode, elapsedTime);
            return;
        }

        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (duration * 1000 + threadWarmupMs);

        Runnable r = () -> {
            long t0 = System.nanoTime();
            int chooser = ThreadLocalRandom.current().nextInt(100);
            int res;
            if (chooser < paymentPercentage) {
                // Submit new payment.
                res = runPayment();
                if (res >= 0) {
                    paymentTimes.add(System.nanoTime() - t0);
                }
            } else {
                // Submit a new order.
                res = runNewOrder();
                if (res >= 0) {
                    newOrderTimes.add(System.nanoTime() - t0);
                }
            }
        };


        while (System.currentTimeMillis() < endTime) {
            long t = System.nanoTime();
            if (System.currentTimeMillis() - startTime < threadWarmupMs) {
                newOrderTimes.clear();
                paymentTimes.clear();
            }
            threadPool.submit(r);
            while (System.nanoTime() - t < interval.longValue() * 1000) {
                // Busy-spin
            }
        }

        long elapsedTime = (System.currentTimeMillis() - startTime) - threadWarmupMs;

        threadPool.shutdownNow();
        threadPool.awaitTermination(10, TimeUnit.SECONDS);

        double totalThroughput = 0.0;

        List<Long> queryTimes = paymentTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        int numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            totalThroughput += throughput;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("Payment: Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No Payment.");
        }

        queryTimes = newOrderTimes.stream().map(i -> i / 1000).sorted().collect(Collectors.toList());
        numQueries = queryTimes.size();
        if (numQueries > 0) {
            long average = queryTimes.stream().mapToLong(i -> i).sum() / numQueries;
            double throughput = (double) numQueries * 1000.0 / elapsedTime;
            totalThroughput += throughput;
            long p50 = queryTimes.get(numQueries / 2);
            long p99 = queryTimes.get((numQueries * 99) / 100);
            logger.info("New Order: Duration: {} Interval: {}μs Queries: {} TPS: {} Average: {}μs p50: {}μs p99: {}μs", elapsedTime, interval, numQueries, String.format("%.03f", throughput), average, p50, p99);
        } else {
            logger.info("No NewOrder");
        }
        logger.info("Total Throughput: {}", totalThroughput);

        apiaryWorker.shutdown();
    }

    private static int runPayment() {
        int districtID = TPCCUtil.randomNumber(1, TPCCConfig.configDistPerWhse, gen);

        int x = TPCCUtil.randomNumber(1, 100, gen);
        int customerDistrictID;
        int customerWarehouseID;
        int w_id = ThreadLocalRandom.current().nextInt(1, numWarehouses + 1);
        if (x <= 85) {
            customerDistrictID = districtID;
            customerWarehouseID = w_id;
        } else {
            customerDistrictID = TPCCUtil.randomNumber(1, TPCCConfig.configDistPerWhse, gen);
            do {
                customerWarehouseID = TPCCUtil.randomNumber(1, numWarehouses, gen);
            } while (customerWarehouseID == w_id && numWarehouses > 1);
        }

        long y = TPCCUtil.randomNumber(1, 100, gen);
        int customerByName;
        String customerLastName = "";
        int customerID = -1;
        if (y <= 60) {
            // 60% lookups by last name
            customerByName = 1;
            customerLastName = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
        } else {
            // 40% lookups by customer ID
            customerByName = 0;
            customerID = TPCCUtil.getCustomerID(gen);
        }

        String paymentAmount = String.valueOf(TPCCUtil.randomNumber(100, 500000, gen) / 100.0);

        int res = -1;
        long currTimestamp = System.currentTimeMillis();
        try {
            res = client.get().executeFunction(TPCCUtil.PAYMENT_FUNC, w_id, districtID, customerID, customerDistrictID, customerWarehouseID, customerByName, customerLastName, paymentAmount, String.valueOf(currTimestamp)).getInt();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return res;
    }

    private static int runNewOrder() {
        int res = -1;

        int terminalWarehouseID = ThreadLocalRandom.current().nextInt(1, numWarehouses + 1);
        int districtID = TPCCUtil.randomNumber(1, TPCCConfig.configDistPerWhse, gen);
        int customerID = TPCCUtil.getCustomerID(gen);
        int numItems = (int) TPCCUtil.randomNumber(5, 15, gen);
        int[] itemIDs = new int[numItems];
        int[] supplierWarehouseIDs = new int[numItems];
        int[] orderQuantities = new int[numItems];
        int allLocal = 1;
        for (int i = 0; i < numItems; i++) {
            itemIDs[i] = TPCCUtil.getItemID(gen);
            if (TPCCUtil.randomNumber(1, 100, gen) > 1) {
                supplierWarehouseIDs[i] = terminalWarehouseID;
            } else {
                do {
                    supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1,
                            numWarehouses, gen);
                } while (supplierWarehouseIDs[i] == terminalWarehouseID
                        && numWarehouses > 1);
                allLocal = 0;
            }
            orderQuantities[i] = TPCCUtil.randomNumber(1, 10, gen);
        }

        // we need to cause 1% of the new orders to be rolled back.
        if (TPCCUtil.randomNumber(1, 100, gen) == 1) {
            itemIDs[numItems - 1] = TPCCConfig.INVALID_ITEM_ID;
        }

        long currTimestamp = System.currentTimeMillis();

        try {
            res = client.get().executeFunction(TPCCUtil.NEWORDER_FUNC, terminalWarehouseID, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs,orderQuantities, String.valueOf(currTimestamp)).getInt();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    private static void resetAllTables(String dbAddr) {
        logger.info("Reset all tables!");
        try {
            PostgresConnection pgConn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "dbos", RetroBenchmark.provenanceDB, RetroBenchmark.provenanceAddr);
            Connection provConn = pgConn.provConnection.get();
            pgConn.dropTable(ProvenanceBuffer.PROV_ApiaryMetadata);
            PostgresConnection.dropTable(provConn, ApiaryConfig.tableFuncInvocations);
            PostgresConnection.dropTable(provConn, ProvenanceBuffer.PROV_QueryMetadata);
            PostgresConnection.dropTable(provConn, ApiaryConfig.tableRecordedInputs);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to Postgres.");
        }
    }
}
