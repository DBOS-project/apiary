package org.dbos.apiary.rsademo.executable;

import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

public class ResetDatabase {

    private static final Logger logger = LoggerFactory.getLogger(ResetDatabase.class);

    public static void resetDatabase(String dbAddr) {
        try {
            PostgresConnection pgConn = new PostgresConnection(dbAddr, ApiaryConfig.postgresPort, "postgres", "dbos", ApiaryConfig.vertica, dbAddr);
            Connection provConn = pgConn.provConnection.get();
            pgConn.dropTable(ProvenanceBuffer.PROV_ApiaryMetadata);
            pgConn.dropTable("WebsiteLogins"); // For testing.
            pgConn.dropTable("WebsitePosts"); // For testing.
            pgConn.createTable("WebsiteLogins", "Username VARCHAR(1000) PRIMARY KEY NOT NULL, Password VARCHAR(1000) NOT NULL");
            pgConn.createTable("WebsitePosts", "Sender VARCHAR(1000) NOT NULL, Receiver VARCHAR(1000) NOT NULL, PostText VARCHAR(10000) NOT NULL");
            PostgresConnection.dropTable(provConn, ApiaryConfig.tableFuncInvocations);
            PostgresConnection.dropTable(provConn, ProvenanceBuffer.PROV_QueryMetadata);
            PostgresConnection.dropTable(provConn, ApiaryConfig.tableRecordedInputs);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to Postgres and reset tables.");
        }
    }
}
