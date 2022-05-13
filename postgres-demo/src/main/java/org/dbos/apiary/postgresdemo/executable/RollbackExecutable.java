package org.dbos.apiary.postgresdemo.executable;

import org.apache.commons.cli.*;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.postgres.PostgresConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class RollbackExecutable {
    private static final Logger logger = LoggerFactory.getLogger(RollbackExecutable.class);

    private static final String websiteLogins = "WebsiteLogins";
    private static final String websitePosts = "WebsitePosts";
    private static final String[] tables = {websiteLogins, websitePosts};
    private static final String provSuffix = "Events";

    // Given a specific UNIX timestamp in microsecond, rollback all inserted records afterwards (including the timestamp) and reset the tables to the state right before the timestamp.
    public static void main(String[] args) throws ParseException, SQLException {
        Options options = new Options();
        options.addOption("timestamp", true, "Unix Timestamp in Microsecond scale.");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        assert (cmd.hasOption("timestamp"));
        long timestamp = Long.parseLong(cmd.getOptionValue("timestamp"));

        // Create a connection to the backend database.
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[] {"localhost"});
        ds.setPortNumbers(new int[] {ApiaryConfig.postgresPort});
        ds.setDatabaseName("postgres");
        ds.setUser("postgres");
        ds.setPassword("dbos");
        ds.setSsl(false);
        Connection conn = ds.getConnection();

        for (String table : tables) {
            rollbackTable(conn, table, timestamp);
        }
        logger.info("Rolled back the database to the state before timestamp: {}!", timestamp);
    }

    private static void rollbackTable(Connection conn, String tableName, long timestamp) throws SQLException {
        Statement stmt = conn.createStatement();
        PreparedStatement pstmt;
        ResultSetMetaData rsmd;
        ResultSet rs;
        int offset = 3;

        // Check inserted records to be deleted.
        rs = stmt.executeQuery(String.format("SELECT * FROM %s WHERE APIARY_TIMESTAMP >= %d AND APIARY_OPERATION_TYPE=%d",
                tableName + provSuffix, timestamp, ProvenanceBuffer.ExportOperation.INSERT.getValue()));
        rsmd = rs.getMetaData();
        int numColumns = rsmd.getColumnCount();
        assert (numColumns > offset);

        // Construct prepared query.
        StringBuilder preparedQuery = new StringBuilder("DELETE FROM " + tableName + " WHERE ");
        String sep = "";
        for (int col = offset+1; col <= numColumns; col++) {
            preparedQuery.append(sep);
            String colName = rsmd.getColumnName(col);
            preparedQuery.append(colName + "=?");
            sep = " AND ";
        }
        preparedQuery.append(";");
        pstmt = conn.prepareStatement(preparedQuery.toString());

        // Delete records.
        int cnt = 0;
        while (rs.next()) {
            for (int col = offset+1; col <= numColumns; col++) {
                Object o;
                o = rs.getObject(col);
                pstmt.setObject(col - offset, o);
            }
            pstmt.executeUpdate();
            cnt++;
        }
        logger.info("Deleted {} records from table {}", cnt, tableName);
    }
}
