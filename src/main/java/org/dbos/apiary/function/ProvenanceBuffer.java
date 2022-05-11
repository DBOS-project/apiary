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

    // TODO: need a better way to auto-reconnect to Vertica, during transient failures.
    public final ThreadLocal<Connection> conn;
    private final String databaseName;

    public final Boolean hasConnection;

    private Thread exportThread;

    public ProvenanceBuffer(String databaseName, String databaseAddress) throws ClassNotFoundException {
        this.databaseName = databaseName;
        if (databaseName.equals("vertica")) {
            Class.forName("com.vertica.jdbc.Driver");
            this.conn = ThreadLocal.withInitial(() -> {
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
                    return c;
                } catch (SQLException e) {

                }
                return null;
            });
        } else {
            assert(databaseName.equals("postgres"));
            this.conn = ThreadLocal.withInitial(() -> {
                // Connect to Postgres.
                PGSimpleDataSource ds = new PGSimpleDataSource();
                ds.setServerNames(new String[] {databaseAddress});
                ds.setPortNumbers(new int[] {ApiaryConfig.postgresPort});
                ds.setDatabaseName("postgres");
                ds.setUser("postgres");
                ds.setPassword("dbos");
                ds.setSsl(false);
                Connection conn;
                try {
                    conn = ds.getConnection();
                    return conn;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            });
        }

        if (conn.get() == null) {
            logger.info("No Vertica instance!");
            this.hasConnection = false;
            return;
        }


        Runnable r = () -> {
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    exportBuffer();
                    Thread.sleep(exportInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        exportThread = new Thread(r);
        exportThread.start();
        this.hasConnection = true;
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
                tableBufferMap.put(table, new TableBuffer(null, null));
            } else {
                String preparedQuery = getPreparedQuery(table, colTypeMap.size());
                tableBufferMap.put(table, new TableBuffer(preparedQuery, colTypeMap));
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
            // Pad the rest with zeros.
            for (int j = numVals; j < numColumns; j++) {
                setColumn(pstmt, j+1, Types.VARCHAR, padding);
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
        logger.info("Exported table {}, {} rows", table, numEntries);
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
        } else if (colType == Types.VARCHAR) {
            pstmt.setString(colIndex, val.toString());
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
        if (databaseName.equals("vertica")) {
            preparedQuery = new StringBuilder("INSERT INTO " + table + " VALUES (");
            for (int i = 0; i < numColumns; i++) {
                if (i != 0) {
                    preparedQuery.append(",");
                }
                preparedQuery.append("?");
            }
        } else {
            assert(databaseName.equals("postgres"));
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
