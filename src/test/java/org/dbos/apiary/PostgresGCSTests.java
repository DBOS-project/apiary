package org.dbos.apiary;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.gcs.GCSConnection;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.gcs.GCSReadString;
import org.dbos.apiary.procedures.gcs.GCSWriteString;
import org.dbos.apiary.procedures.postgres.pggcs.PostgresReadString;
import org.dbos.apiary.procedures.postgres.pggcs.PostgresWriteString;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PostgresGCSTests {
    private static final Logger logger = LoggerFactory.getLogger(PostgresGCSTests.class);

    private ApiaryWorker apiaryWorker;

    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
            conn.dropTable("FuncInvocations");
            conn.dropTable("StuffTable");
            conn.dropTable("VersionTable");
            conn.createTable("StuffTable", "Name varchar(1000) PRIMARY KEY NOT NULL, Stuff varchar(1000) NOT NULL");
            conn.createTable("VersionTable", "Name varchar(1000), Version integer NOT NULL");
            conn.createIndex("CREATE INDEX VersionIndex ON VersionTable (Name, Version);");
        } catch (Exception e) {
            logger.info("Failed to connect to Postgres.");
        }
        apiaryWorker = null;
    }

    @AfterEach
    public void cleanupWorker() {
        if (apiaryWorker != null) {
            apiaryWorker.shutdown();
        }
    }

    @BeforeEach
    public void cleanupGCS() {
        try {
            Storage storage = StorageOptions.getDefaultInstance().getService();
            Bucket bucket = storage.get(ApiaryConfig.gcsTestBucket);
            for (Blob blob : bucket.list().iterateAll()) {
                blob.delete();
            }
        } catch (Exception e) {
            logger.info("No GCS instance! {}", e.getMessage());
        }
    }


    @Test
    public void testGCSBasic() throws InvalidProtocolBufferException {
        logger.info("testGCSBasic");

        GCSConnection conn;
        PostgresConnection pconn;
        try {
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
            conn = new GCSConnection(pconn);
        } catch (Exception e) {
            logger.info("No GCS/Postgres instance! {}", e.getMessage());
            return;
        }

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.gcs, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresWriteString", ApiaryConfig.postgres, PostgresWriteString::new);
        apiaryWorker.registerFunction("PostgresReadString", ApiaryConfig.postgres, PostgresReadString::new);
        apiaryWorker.registerFunction("GCSWriteString", ApiaryConfig.gcs, GCSWriteString::new);
        apiaryWorker.registerFunction("GCSReadString", ApiaryConfig.gcs, GCSReadString::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        String res = client.executeFunction("PostgresWriteString", "matei", "matei zaharia").getString();
        assertEquals("matei", res);

        int resnum = client.executeFunction("PostgresReadString", "matei").getInt();
        assertEquals(0, resnum);
    }

    @Test
    public void testGCSConcurrent() throws InterruptedException {
        logger.info("testGCSConcurrent");

        GCSConnection conn;
        PostgresConnection pconn;
        try {
            pconn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
            conn = new GCSConnection(pconn);
        } catch (Exception e) {
            logger.info("No GCS/Postgres instance! {}", e.getMessage());
            return;
        }

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(ApiaryConfig.gcs, conn);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, pconn);
        apiaryWorker.registerFunction("PostgresWriteString", ApiaryConfig.postgres, PostgresWriteString::new);
        apiaryWorker.registerFunction("PostgresReadString", ApiaryConfig.postgres, PostgresReadString::new);
        apiaryWorker.registerFunction("GCSWriteString", ApiaryConfig.gcs, GCSWriteString::new);
        apiaryWorker.registerFunction("GCSReadString", ApiaryConfig.gcs, GCSReadString::new);
        apiaryWorker.startServing();

        int numThreads = 10;
        long start = System.currentTimeMillis();
        long testDurationMs = 5000L;
        AtomicInteger count = new AtomicInteger(0);
        AtomicBoolean success = new AtomicBoolean(true);
        Runnable r = () -> {
            try {
                ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");
                while (System.currentTimeMillis() < start + testDurationMs) {
                    int localCount = count.getAndIncrement();
                    client.executeFunction("PostgresWriteString", "matei" + localCount, Integer.toString(localCount)).getInt();
                    String search = "matei" + ThreadLocalRandom.current().nextInt(localCount - 5, localCount + 5);
                    int res = client.executeFunction("PostgresReadString", search).getInt();
                    if (res == -1) {
                        success.set(false);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                success.set(false);
            }
        };

        List<Thread> threads = new ArrayList<>();
        for (int threadNum = 0; threadNum < numThreads; threadNum++) {
            Thread t = new Thread(r);
            threads.add(t);
            t.start();
        }
        for (Thread t: threads) {
            t.join();
        }
        assertTrue(success.get());
    }
}
