package org.dbos.apiary.interposition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.catalog.Table;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

// Buffer provenance/log messages and export to a remote OLAP database.
public class ProvenanceBuffer {
    private static final Logger logger = LoggerFactory.getLogger(ProvenanceBuffer.class);

    public static final int batchSize = 100000;  // TODO: configurable?
    public static final int commitSize = 1000000;  // TODO: configurable?
    public static final String padding = "0";
    public static final int exportInterval = 1000;

    public final ThreadLocal<Connection> conn;

    public ProvenanceBuffer(String olapDBaddr) throws ClassNotFoundException {
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
                        String.format("jdbc:vertica://%s/apiary_provenance", olapDBaddr),
                        verticaProp
                );
                c.setAutoCommit(false);
                return c;
            } catch (SQLException e) {
                
            }
            return null;
        });

        if (conn.get() == null) {
            logger.info("No Vertica instance!");
            return;
        }

        Runnable r = () -> {
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    exportBuffer();
                    Thread.sleep(exportInterval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        Thread exportThread = new Thread(r);
        exportThread.start();
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
            String preparedQuery= getPreparedQuery(table, colTypeMap.size());
            tableBufferMap.put(table, new TableBuffer(preparedQuery, colTypeMap));
        }
        tableBufferMap.get(table).bufferEntryQueue.add(objects);
    }

    private void exportBuffer() {
        for (String table : tableBufferMap.keySet()) {
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
        TableBuffer tableBuffer = tableBufferMap.get(table);
        PreparedStatement pstmt = connection.prepareStatement(tableBuffer.preparedQuery);
        int numEntries = tableBuffer.bufferEntryQueue.size();

        int rowCnt = 0;
        int commitCnt = 0;
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
            commitCnt++;
            if (rowCnt >= batchSize) {
                pstmt.executeBatch();
                rowCnt = 0;
            }
            if (commitCnt >= commitSize) {
                connection.commit();
                commitCnt = 0;
            }
        }
        if (rowCnt > 0) {
            pstmt.executeBatch();
        }
        if (commitCnt > 0) {
            connection.commit();
        }
        logger.info("Exported table {}, {} rows", table, numEntries);
    }

    private static void setColumn(PreparedStatement pstmt, int colIndex, int colType, Object val) throws SQLException {
        // Convert value to the target type.
        // TODO: support more types. Vertica treats all integer as BIGINT.
        if (colType == Types.BIGINT) {
            long longval = 0l;
            if (val instanceof Long) {
                longval = ((Long) val).longValue();
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

    private String getPreparedQuery(String table, int numColumns) {
        StringBuilder preparedQuery = new StringBuilder("INSERT INTO " + table + " VALUES (");
        for (int i = 0; i < numColumns; i++) {
            if (i != 0) {
                preparedQuery.append(",");
            }
            preparedQuery.append("?");
        }
        preparedQuery.append(")");
        return preparedQuery.toString();
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
            e.printStackTrace();
            return null;
        }
        return colTypeMap;
    }
}
