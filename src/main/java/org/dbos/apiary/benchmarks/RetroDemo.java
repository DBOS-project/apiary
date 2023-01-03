package org.dbos.apiary.benchmarks;

import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetroDemo {
    private static final Logger logger = LoggerFactory.getLogger(RetroDemo.class);

    private static final int threadPoolSize = 4;
    private static final int numWorker = 4;

    // Users can subscribe to forums.
    private static final int numUsers = 10;
    private static final int numForums = 10;

    private static final int initialUserId = 11;
    private static final int initialForumId = 2;

    public static void benchmark(String dbAddr, int replayMode, long targetExecId) {
        ApiaryConfig.isolationLevel = ApiaryConfig.SERIALIZABLE;
        ApiaryConfig.XDBTransactions = false;

        if (replayMode == ApiaryConfig.ReplayMode.NOT_REPLAY.getValue()) {
            ApiaryConfig.recordInput = true;
            // Reset all tables if we do initial runs.
            resetAllTables(dbAddr);
        } else {
            ApiaryConfig.recordInput = false;
            // Only reset the app table.
            resetAllTables(dbAddr);
        }
    }

    private static void resetAllTables(String dbAddr) {
        try {
            PostgresConnection pgConn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "dbos");

            pgConn.dropTable("ForumSubscription");
            pgConn.createTable("ForumSubscription", "UserId integer NOT NULL, ForumId integer NOT NULL");
            pgConn.dropTable(ApiaryConfig.tableFuncInvocations);
            pgConn.dropTable(ProvenanceBuffer.PROV_ApiaryMetadata);
            pgConn.dropTable(ProvenanceBuffer.PROV_QueryMetadata);
            pgConn.dropTable(ApiaryConfig.tableRecordedInputs);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to Postgres.");
        }
    }

    private static void resetAppTables(String dbAddr) {
        try {
            PostgresConnection pgConn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "dbos");
            pgConn.truncateTable("ForumSubscription", false);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to Postgres.");
        }
    }
}
