package org.dbos.apiary;

import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.wordpress.WPUtil;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

// To test the bug and fixes of WordPress-11073: https://core.trac.wordpress.org/ticket/11073
public class WordPressTests {
    private static final Logger logger = LoggerFactory.getLogger(WordPressTests.class);

    private ApiaryWorker apiaryWorker;

    @BeforeAll
    public static void testConnection() {
        assumeTrue(TestUtils.testPostgresConnection());
        // Set the isolation level to serializable.
        ApiaryConfig.isolationLevel = ApiaryConfig.SERIALIZABLE;

        // Disable XDB transactions.
        ApiaryConfig.XDBTransactions = false;

        // Disable read tracking.
        ApiaryConfig.captureReads = false;
    }

    @BeforeEach
    public void resetTables() {
        try {
            PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos");
            conn.dropTable(ApiaryConfig.tableFuncInvocations);
            conn.dropTable(ApiaryConfig.tableRecordedInputs);
            conn.dropTable(ProvenanceBuffer.PROV_ApiaryMetadata);
            conn.dropTable(ProvenanceBuffer.PROV_QueryMetadata);
            conn.dropTable(WPUtil.WP_POSTS_TABLE);
            conn.createTable(WPUtil.WP_POSTS_TABLE, WPUtil.WP_POSTS_SCHEMA);
            conn.dropTable(WPUtil.WP_POSTMETA_TABLE);
            conn.createTable(WPUtil.WP_POSTMETA_TABLE, WPUtil.WP_POSTMETA_SCHEMA);
            conn.dropTable(WPUtil.WP_COMMENTS_TABLE);
            conn.createTable(WPUtil.WP_COMMENTS_TABLE, WPUtil.WP_COMMENTS_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to Postgres.");
            assumeTrue(false);
        }
        apiaryWorker = null;
    }

    @AfterEach
    public void cleanupWorker() {
        if (apiaryWorker != null) {
            apiaryWorker.shutdown();
        }
    }

    @Test
    public void testWPSerialized() throws SQLException {
        logger.info("testWPSerialized");
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
    }
}
