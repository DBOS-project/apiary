package org.dbos.apiary;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.procedures.postgres.wordpress.*;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    public void testWPSerialized() throws SQLException, InvalidProtocolBufferException, InterruptedException {
        logger.info("testWPSerialized");
        PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, ApiaryConfig.postgres, "dbos");

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4, ApiaryConfig.postgres, ApiaryConfig.provenanceDefaultAddress);
        apiaryWorker.registerConnection(ApiaryConfig.postgres, conn);
        apiaryWorker.registerFunction("WPAddPost", ApiaryConfig.postgres, WPAddPost::new);
        apiaryWorker.registerFunction("WPAddComment", ApiaryConfig.postgres, WPAddComment::new);
        apiaryWorker.registerFunction("WPGetPostComments", ApiaryConfig.postgres, WPGetPostComments::new);
        apiaryWorker.registerFunction("WPTrashPost", ApiaryConfig.postgres, WPTrashPost::new);
        apiaryWorker.registerFunction("WPTrashComments", ApiaryConfig.postgres, WPTrashComments::new);
        apiaryWorker.startServing();
        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("WPAddComment", 123, 3450, "this should not work.").getInt();
        assertEquals(-1, res);
        res = client.executeFunction("WPAddPost", 123, "test post").getInt();
        assertEquals(0, res);
        res = client.executeFunction("WPAddComment", 123, 3450, "test comment to a post.").getInt();
        assertEquals(0, res);
        res = client.executeFunction("WPAddComment", 123, 3460, "second test comment to a post.").getInt();
        assertEquals(0, res);

        String[] resList = client.executeFunction("WPGetPostComments", 123).getStringArray();
        assertEquals(3, resList.length);
        assertTrue(resList[0].equals("test post"));
        assertTrue(resList[1].equals("test comment to a post."));
        assertTrue(resList[2].equals("second test comment to a post."));

        // Trash the post.
        res = client.executeFunction("WPTrashPost", 123).getInt();
        assertEquals(123, res);
        
        // Check provenance.
        Thread.sleep(ProvenanceBuffer.exportInterval * 2);
    }
}
