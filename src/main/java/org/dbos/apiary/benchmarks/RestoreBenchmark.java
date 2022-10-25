package org.dbos.apiary.benchmarks;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.replay.PostgresCountTable;
import org.dbos.apiary.procedures.postgres.replay.PostgresInsertMany;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RestoreBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(RestoreBenchmark.class);

    private static final int threadPoolSize = 4;
    private static final int numWorker = 4;
    private static final int chunkSize = 1000;

    public static void benchmark(String dbAddr, int numTables, int numColumns, int numRows) throws SQLException, InterruptedException {
        // Reset tables.
        resetTables(dbAddr, numTables, numColumns);

        PostgresConnection pgConn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");

        ApiaryWorker apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), numWorker);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pgConn);

        apiaryWorker.registerFunction("PostgresInsertMany", ApiaryConfig.postgres, PostgresInsertMany::new);
        apiaryWorker.registerFunction("PostgresCountTable", ApiaryConfig.postgres, PostgresCountTable::new);
        apiaryWorker.startServing();

        ThreadLocal<ApiaryWorkerClient> client = ThreadLocal.withInitial(() -> new ApiaryWorkerClient("localhost"));

        long loadStart = System.currentTimeMillis();

        int numChunks = numRows / chunkSize;
        AtomicInteger tableNum = new AtomicInteger(0);
        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolSize);

        Runnable r = () -> {
            long startTime = System.currentTimeMillis();
            int localTableNum = tableNum.incrementAndGet();
            assert (localTableNum <= numTables);
            String tableName = "tbl" + localTableNum;
            // Bulk load the table.
            int count = 0;
            int res;
            for (int chunkNum = 0; chunkNum < numChunks; chunkNum++) {
                int[] numbers = new int[chunkSize];
                for (int i = 0; i < chunkSize; i++) {
                    numbers[i] = count;
                    count++;
                }
                try {
                    res = client.get().executeFunction("PostgresInsertMany", tableName, numColumns, numbers).getInt();
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                assert(res == numbers.length);
            }
            // Check actually loaded the table.
            try {
                res = client.get().executeFunction("PostgresCountTable", tableName).getInt();
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            assert(res == numRows);
            logger.info("Done loading table: {}, {} ms", tableName, System.currentTimeMillis() - startTime);
        };

        // Load all tables in parallel.
        for (int i = 0; i < numTables; i++) {
            threadPool.submit(r);
        }
        threadPool.shutdown();
        threadPool.awaitTermination(100000, TimeUnit.SECONDS);
        logger.info("Done loading {} tables: {} ms", numTables, System.currentTimeMillis() - loadStart);

        apiaryWorker.shutdown();
    }

    private static void resetTables(String dbAddr, int numTables, int numColumns) {
        assert (numTables > 0);
        assert (numColumns > 0);
        try {
            PostgresConnection conn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
            conn.dropTable(ProvenanceBuffer.PROV_FuncInvocations);

            for (int i = 1; i <= numTables; i++) {
                conn.dropTable("tbl" + i);
                StringBuilder spec = new StringBuilder("col1 BIGINT NOT NULL");
                for (int j = 2; j <= numColumns; j++) {
                    spec.append(", col" + j + " BIGINT NOT NULL");
                }
                conn.createTable("tbl" + i, spec.toString());
            }
            conn.dropTable(ProvenanceBuffer.PROV_ApiaryMetadata);
            conn.dropTable(ProvenanceBuffer.PROV_QueryMetadata);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to Postgres.");
        }
    }
}
