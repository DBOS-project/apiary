package org.dbos.apiary.function;

import org.dbos.apiary.utilities.ApiaryConfig;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * For internal use only.
 * Buffer provenance/log messages and export to an OLAP database.
 */
public class ProvenanceBuffer {
    private static final Logger logger = LoggerFactory.getLogger(ProvenanceBuffer.class);

    public static final int batchSize = 100000;  // TODO: configurable?
    public static final String padding = "0";
    public static final int exportInterval = 1000;

    // Constant variables.
    public static final String PROV_ApiaryMetadata = "ApiaryMetadata";
    public static final String PROV_QueryMetadata = "ApiaryQueryMetadata";
    public static final String PROV_APIARY_TRANSACTION_ID = "APIARY_TRANSACTION_ID";
    public static final String PROV_APIARY_TIMESTAMP = "APIARY_TIMESTAMP";
    public static final String PROV_EXECUTIONID = "APIARY_EXECUTIONID";
    public static final String PROV_FUNCID ="APIARY_FUNCID";
    public static final String PROV_APIARY_ROLE = "APIARY_ROLE";
    public static final String PROV_PROCEDURENAME = "APIARY_PROCEDURENAME";
    public static final String PROV_ISREPLAY = "APIARY_ISREPLAY";
    public static final String PROV_APIARY_OPERATION_TYPE = "APIARY_OPERATION_TYPE";
    public static final String PROV_QUERY_STRING = "APIARY_QUERY_STRING";
    public static final String PROV_QUERY_SEQNUM = "APIARY_QUERY_SEQNUM";
    public static final String PROV_QUERY_TABLENAMES = "APIARY_QUERY_TABLENAMES";
    public static final String PROV_QUERY_PROJECTION = "APIARY_QUERY_PROJECTION";
    public static final String PROV_REQ_BYTES = "APIARY_REQ_BYTES";
    public static final String PROV_END_TIMESTAMP = "APIARY_END_TIMESTAMP";
    // For the status column.
    public static final String PROV_FUNC_STATUS = "APIARY_FUNC_STATUS";
    // For the transaction snapshot column.
    public static final String PROV_TXN_SNAPSHOT = "APIARY_TXN_SNAPSHOT";
    public static final String PROV_READONLY = "APIARY_READONLY";
    public static final String PROV_STATUS_COMMIT = "commit";
    public static final String PROV_STATUS_FAIL_RECOVERABLE = "fail_recoverable";
    public static final String PROV_STATUS_FAIL_UNRECOVERABLE = "fail_unrecoverable";
    public static final String PROV_STATUS_EMBEDDED = "embedded";
    public static final String PROV_STATUS_REPLAY = "replayed";

    public boolean shouldStop = false;
    /**
     * Enum class for provenance operations.
     */
    public enum ExportOperation {
        INSERT(1),
        DELETE(2),
        UPDATE(3),
        READ(4);

        private int value;

        private ExportOperation(int value) {
            this.value = value;
        }

        public int getValue() {
            return this.value;
        }
    }

    // TODO: need a better way to auto-reconnect to the remote provenance DB, during transient failures.
    public final ThreadLocal<Connection> conn;
    private final String databaseType;

    public final Boolean hasConnection;

    private Thread exportThread;

    public ThreadLocal<Connection> pgConn = null;  // Connect to postgres;

    public ProvenanceBuffer(String databaseType, String databaseAddress) throws ClassNotFoundException {
        this.databaseType = databaseType;
        if (databaseType == null) {
            logger.debug("No provenance buffer!");
            this.conn = null;
            this.hasConnection = false;
            return;
        }
        this.conn = ThreadLocal.withInitial(() -> createProvConnection(databaseType, databaseAddress));

        if (conn.get() == null) {
            logger.info("No DB instance for provenance!");
            this.hasConnection = false;
            return;
        }

        Runnable r = () -> {
            while(!shouldStop) {
                try {
                    exportBuffer();
                    Thread.sleep(exportInterval);
                } catch (InterruptedException e) {
                    shouldStop = true;
                }
            }
            // Export one last time.
            exportBuffer();
        };
        exportThread = new Thread(r);
        exportThread.start();
        this.hasConnection = true;
    }

    public static Connection createProvConnection(String databaseType, String databaseAddress) {
        if (databaseType.equals(ApiaryConfig.vertica)) {
            try {
                Class.forName("com.vertica.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                return null;
            }
            // Connect to Vertica.
            Properties verticaProp = new Properties();
            verticaProp.put("user", "dbadmin");
            verticaProp.put("password", "password");
            verticaProp.put("loginTimeout", "35");
            verticaProp.put("streamingBatchInsert", "True");
            verticaProp.put("ConnectionLoadBalance", "1"); // Enable load balancing.
            try {
                Connection c = DriverManager.getConnection(
                        String.format("jdbc:vertica://%s/apiary_provenance", databaseAddress),
                        verticaProp
                );
                c.setAutoCommit(true);
                return c;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            assert(databaseType.equals(ApiaryConfig.postgres));
            // Connect to Postgres.
            PGSimpleDataSource ds = new PGSimpleDataSource();
            ds.setServerNames(new String[] {databaseAddress});
            ds.setPortNumbers(new int[] {ApiaryConfig.provenancePort});
            ds.setDatabaseName(ApiaryConfig.dbosDBName);
            ds.setUser("postgres");
            ds.setPassword("dbos");
            ds.setSsl(false);
            Connection conn;
            try {
                conn = ds.getConnection();
                conn.setAutoCommit(true);
                return conn;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public void close() {
        // Close the buffer.
        if (exportThread == null) {
            return;
        }
        try {
            exportThread.interrupt();
            exportThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static class TableBuffer {
        public final String preparedQuery;
        public final Map<Integer, Integer> colTypeMap; // In JDBC, first column starts from 1, not 0.
        public final Queue<Object[]> bufferEntryQueue = new ConcurrentLinkedQueue<>();

        public TableBuffer (String preparedQuery, Map<Integer, Integer> colTypeMap) {
            this.preparedQuery = preparedQuery;
            this.colTypeMap = colTypeMap;
        }
    }

    private final Map<String, TableBuffer> tableBufferMap = new ConcurrentHashMap<>();

    public void addEntry(String table, Object... objects) {
        if (!tableBufferMap.containsKey(table)) {
            Map<Integer, Integer> colTypeMap = getColTypeMap(table);
            if ((colTypeMap == null) || colTypeMap.isEmpty()) {
                // Do not capture provenance.
                tableBufferMap.putIfAbsent(table, new TableBuffer(null, null));
            } else {
                String preparedQuery = getPreparedQuery(table, colTypeMap.size());
                tableBufferMap.putIfAbsent(table, new TableBuffer(preparedQuery, colTypeMap));
            }
        }
        if (tableBufferMap.get(table).preparedQuery != null) {
            tableBufferMap.get(table).bufferEntryQueue.add(objects);
        }
    }

    private void exportBuffer() {
        for (String table : tableBufferMap.keySet()) {
            if (tableBufferMap.get(table).preparedQuery == null) {
                continue;
            }
            try {
                if (!tableBufferMap.get(table).bufferEntryQueue.isEmpty()) {
                    exportTableBuffer(table);
                }
            } catch (SQLException e) {
                e.printStackTrace();
                logger.error("Failed to export table {}", table);
            }
        }
    }

    private void exportTableBuffer(String table) throws SQLException {
        Connection connection = this.conn.get();
        connection.setAutoCommit(false);
        if (connection == null) {
            logger.error("Failed to get connection.");
            return;
        }
        TableBuffer tableBuffer = tableBufferMap.get(table);
        PreparedStatement pstmt = connection.prepareStatement(tableBuffer.preparedQuery);
        int numEntries = tableBuffer.bufferEntryQueue.size();

        int rowCnt = 0;
        for (int i = 0; i < numEntries; i++) {
            Object[] listVals = tableBuffer.bufferEntryQueue.poll();
            if (listVals == null) {
                break;
            }
            int numVals = listVals.length;
            int numColumns = tableBuffer.colTypeMap.size();
            assert (numVals <= numColumns);

            for (int j = 0; j < numVals; j++) {
                // Column index starts with 1.
                setColumn(pstmt, j+1, tableBuffer.colTypeMap.get(j+1), listVals[j]);
            }
            // Pad the rest with zeros if it's Vertica, because it doesn't support NULL very well.
            for (int j = numVals; j < numColumns; j++) {
                if (databaseType.equals(ApiaryConfig.vertica)) {
                    setColumn(pstmt, j + 1, Types.VARCHAR, padding);
                } else {
                    setColumn(pstmt, j + 1, Types.NULL, null);
                }
            }
            pstmt.addBatch();
            rowCnt++;
            if (rowCnt >= batchSize) {
                pstmt.executeBatch();
                rowCnt = 0;
            }
        }
        if (rowCnt > 0) {
            pstmt.executeBatch();
        }
        connection.commit();
        connection.setAutoCommit(true);
        logger.debug("Exported table {}, {} rows", table, numEntries);
    }

    private static void setColumn(PreparedStatement pstmt, int colIndex, int colType, Object val) throws SQLException {
        // Convert value to the target type.
        if (val == null) {
            // The column must be nullable.
            pstmt.setNull(colIndex, colType);
            return;
        }
        if (colType == Types.INTEGER) {
            int intval = 0;
            if (val instanceof Integer) {
                intval = (Integer) val;
            } else if (val instanceof Short) {
                intval = ((Short) val).intValue();
            } else if (val instanceof Byte) {
                intval = ((Byte) val).intValue();
            }
            pstmt.setInt(colIndex, intval);
        } else if (colType == Types.BIGINT) {
            long longval = 0L;
            if (val instanceof Long) {
                longval = (Long) val;
            } else if (val instanceof Integer) {
                longval = ((Integer) val).longValue();
            } else if (val instanceof Short) {
                longval = ((Short) val).longValue();
            } else if (val instanceof Byte) {
                longval = ((Byte) val).longValue();
            }
            pstmt.setLong(colIndex, longval);
        } else if (colType == Types.SMALLINT) {
            short smallVal = 0;
            if (val instanceof Long) {
                smallVal = ((Long) val).shortValue();
            } else if (val instanceof Integer) {
                smallVal = ((Integer) val).shortValue();
            } else if (val instanceof Short) {
                smallVal = (Short) val;
            } else if (val instanceof Byte) {
                smallVal = ((Byte) val).shortValue();
            }
            pstmt.setLong(colIndex, smallVal);
        } else if (colType == Types.VARCHAR) {
            pstmt.setString(colIndex, val.toString());
        } else if ((colType == Types.BINARY) || (colType == Types.VARBINARY)) {
            // The bytea type.
            pstmt.setBytes(colIndex, (byte[]) val);
        } else if ((colType == Types.BOOLEAN) || (colType == Types.BIT)) {
            // Somehow Postgres JDBC uses BIT type.
            boolean boolVal = false;
            if (val instanceof Boolean) {
                boolVal = (Boolean) val;
            } else if (val instanceof String) {
                boolVal = Boolean.parseBoolean((String) val);
            } else {
                boolVal = ((Long) val == 0);
            }
            pstmt.setBoolean(colIndex, boolVal);
        } else {
            // Everything else will be passed directly as string.
            logger.warn(String.format("Failed to convert type: %d. Use String", colType));
            pstmt.setString(colIndex, val.toString());
        }
    }

    private final Map<String, String> preparedQueries = new HashMap<>();

    private String getPreparedQuery(String table, int numColumns) {
        if (preparedQueries.containsKey(table)) {
            return preparedQueries.get(table);
        }
        StringBuilder preparedQuery;
        if (databaseType.equals(ApiaryConfig.vertica)) {
            preparedQuery = new StringBuilder("INSERT INTO " + table + " VALUES (");
            for (int i = 0; i < numColumns; i++) {
                if (i != 0) {
                    preparedQuery.append(",");
                }
                preparedQuery.append("?");
            }
        } else {
            assert(databaseType.equals(ApiaryConfig.postgres));
            preparedQuery = new StringBuilder("INSERT INTO " + table + " (");
            List<String> columnNames = getColNames(table);
            for (int i = 0; i < numColumns; i++) {
                if (i != 0) {
                    preparedQuery.append(",");
                }
                preparedQuery.append(columnNames.get(i));
            }
            preparedQuery.append(") VALUES (");
            for (int i = 0; i < numColumns; i++) {
                if (i != 0) {
                    preparedQuery.append(",");
                }
                preparedQuery.append("?");
            }
        }
        preparedQuery.append(")");
        preparedQueries.put(table, preparedQuery.toString());
        return preparedQuery.toString();
    }

    private List<String> getColNames(String table) {
        if (table.equalsIgnoreCase(ProvenanceBuffer.PROV_ApiaryMetadata + "Events")) {
            // Do not capture provenance for metadata table.
            return null;
        }
        List<String> colNames = new ArrayList<>();
        try {
            Statement stmt = conn.get().createStatement();
            ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s LIMIT 1;", table));
            ResultSetMetaData rsmd = rs.getMetaData();
            int numColumns = rsmd.getColumnCount();
            assert (numColumns > 0);
            for (int i = 1; i <= numColumns; i++) {
                colNames.add(rsmd.getColumnName(i));
            }
        } catch (SQLException e) {
            logger.info("Cannot get table info: {}", table);
            return null;
        }
        return colNames;
    }

    private Map<Integer, Integer> getColTypeMap(String table) {
        if (table.equalsIgnoreCase(ProvenanceBuffer.PROV_ApiaryMetadata + "Events")) {
            // Do not capture provenance for metadata table.
            return null;
        }
        Map<Integer, Integer> colTypeMap = new HashMap<>();
        try {
            Statement stmt = conn.get().createStatement();
            ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s LIMIT 1;", table));
            ResultSetMetaData rsmd = rs.getMetaData();
            int numColumns = rsmd.getColumnCount();
            assert (numColumns > 0);
            for (int i = 1; i <= numColumns; i++) {
                colTypeMap.put(i, rsmd.getColumnType(i));
            }
        } catch (SQLException e) {
            logger.info("Cannot get table info: {}", table);
            return null;
        }
        return colTypeMap;
    }
}
