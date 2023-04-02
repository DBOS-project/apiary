package org.dbos.apiary.postgres;

import org.dbos.apiary.connection.ApiarySecondaryConnection;
import org.dbos.apiary.function.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * PostgresContext is a context for Apiary-Postgres functions.
 * It provides methods for accessing a Postgres database.
 */
public class PostgresContext extends ApiaryContext {
    private static final Logger logger = LoggerFactory.getLogger(PostgresContext.class);
    // This connection ties to all prepared statements in one transaction.
    final Connection conn;
    final Statement stmt;  // A shared statement.
    public PreparedStatement currPstmt = null;  // Current prepared statement, can be used to cancel a running process.

    private final long replayTxID;  // The replayed transaction ID.

    public final Set<String> replayWrittenTables;

    private static final String checkReplayTxID = String.format("SELECT %s FROM %s WHERE %s=? AND %s=? AND %s=0", ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID,
            ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_EXECUTIONID, ProvenanceBuffer.PROV_FUNCID, ProvenanceBuffer.PROV_ISREPLAY);

    private static final String checkMetadata = String.format("SELECT * FROM %s WHERE %s=? AND %s=?", ProvenanceBuffer.PROV_QueryMetadata, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_QUERY_SEQNUM);

    public TransactionContext txc;

    Map<String, Map<String, List<String>>> secondaryWrittenKeys = new HashMap<>();

    public PostgresContext(Connection c, WorkerContext workerContext, String role, long execID, long functionID,
                           int replayMode,
                           Set<TransactionContext> activeTransactions, Set<TransactionContext> abortedTransactions,
                           Set<String> replayWrittenTables) {
        super(workerContext, role, execID, functionID, replayMode);
        this.conn = c;
        try {
            this.stmt = conn.createStatement();
        } catch (SQLException e) {
            logger.error("Failed to create statement.");
            e.printStackTrace();
            throw new RuntimeException("Failed to create statement.");
        }
        long tmpReplayTxID = -1;
        this.replayWrittenTables = replayWrittenTables;
        try {
            long txID = -1;
            long xmin = -1;
            long xmax = -1;
            List<Long> activeTxIDs = new ArrayList<>();
            ResultSet rs = stmt.executeQuery("select pg_current_xact_id();");
            rs.next();
            txID = rs.getLong(1);
            rs.close();
            if ((workerContext.provBuff != null) || ApiaryConfig.XDBTransactions) {
                // Only look up transaction ID and snapshot info if we enable provenance capture.
                rs = stmt.executeQuery("select pg_current_snapshot();");
                rs.next();
                String snapshotString = rs.getString(1);
                xmin = PostgresUtilities.parseXmin(snapshotString);
                long currXmax = PostgresUtilities.parseXmax(snapshotString);
                xmax = currXmax;
                activeTxIDs = PostgresUtilities.parseActiveTransactions(snapshotString);
                rs.close();
                // For epoxy transactions only.
                if (ApiaryConfig.XDBTransactions) {
                    activeTxIDs.addAll(abortedTransactions.stream().map(t -> t.txID).filter(t -> t < currXmax).collect(Collectors.toList()));
                    for (TransactionContext t : activeTransactions) {
                        if (t.txID < xmax && !activeTxIDs.contains(t.txID)) {
                            rs = stmt.executeQuery("select txid_status(" + t.txID + ");");
                            rs.next();
                            if (rs.getString("txid_status").equals("aborted")) {
                                activeTxIDs.add(t.txID);
                            }
                            rs.close();
                        }
                    }
                }
            }
            this.txc = new TransactionContext(txID, xmin, xmax, activeTxIDs);

            // Look up the original transaction ID if it's a single replay.
            if (replayMode == ApiaryConfig.ReplayMode.SINGLE.getValue()) {
                currPstmt = conn.prepareStatement(checkReplayTxID);
                currPstmt.setLong(1, execID);
                currPstmt.setLong(2, functionID);
                rs = currPstmt.executeQuery();
                if (rs.next()) {
                    tmpReplayTxID = rs.getLong(1);
                    logger.debug("Current transaction {} is a replay of executionID: {}, functionID: {}, original tranasction ID: {}", txID, execID, functionID, tmpReplayTxID);
                } else {
                    throw new RuntimeException("Cannot find the original transaction ID for this replay!");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.replayTxID = tmpReplayTxID;
    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) throws Exception {
        ApiaryFunction f = workerContext.getFunction(name);
        String functionType = workerContext.getFunctionType(name);
        if (functionType.equals(ApiaryConfig.postgres) || functionType.equals(ApiaryConfig.stateless)) {
            // Record invocation.
            long startTime = Utilities.getMicroTimestamp();
            FunctionOutput fo = f.apiaryRunFunction(this, inputs);
            if ((workerContext.provBuff != null) && (execID != 0l)) {
                long endTime = Utilities.getMicroTimestamp();
                workerContext.provBuff.addEntry(ApiaryConfig.tableFuncInvocations, txc.txID, startTime, execID, functionID, (short) replayMode, role, name, endTime, ProvenanceBuffer.PROV_STATUS_EMBEDDED);
            }
            return fo;
        } else {
            ApiarySecondaryConnection c = workerContext.getSecondaryConnection(functionType);
            long newID = ((this.functionID + calledFunctionID.incrementAndGet()) << 4);
            Map<String, List<String>> writtenKeys = new HashMap<>();
            try {
                FunctionOutput fo = c.callFunction(name, writtenKeys, workerContext, txc, role, execID, newID, inputs);
                secondaryWrittenKeys.putIfAbsent(functionType, new HashMap<>());
                for (String table: writtenKeys.keySet()) {
                    secondaryWrittenKeys.get(functionType).putIfAbsent(table, new ArrayList<>());
                    secondaryWrittenKeys.get(functionType).get(table).addAll(writtenKeys.get(table));
                }
                return fo;
            } catch (Exception e) {
                secondaryWrittenKeys.putIfAbsent(functionType, new HashMap<>());
                for (String table: writtenKeys.keySet()) {
                    secondaryWrittenKeys.get(functionType).putIfAbsent(table, new ArrayList<>());
                    secondaryWrittenKeys.get(functionType).get(table).addAll(writtenKeys.get(table));
                }
                throw e;
            }
        }
    }

    private void prepareStatement(PreparedStatement ps, Object[] input) throws SQLException {
        for (int i = 0; i < input.length; i++) {
            Object o = input[i];
            if (o instanceof Integer) {
                ps.setInt(i + 1, (Integer) o);
            } else if (o instanceof String) {
                ps.setString(i + 1, (String) o);
            } else if (o instanceof Long)  {
                ps.setLong(i + 1, (Long) o);
            } else if (o instanceof Float) {
                ps.setFloat(i + 1, (Float) o);
            } else if (o instanceof Double) {
                ps.setDouble(i + 1, (Double) o);
            } else if (o instanceof Timestamp) {
                ps.setTimestamp(i + 1, (Timestamp) o);
            } else {
                logger.warn("type {} for input {} not recognized ", o.toString(), i);
                assert (false); // TODO: More types.
            }
        }
    }

    /**
     * Execute a database update.
     * @param procedure a SQL DML statement (e.g., INSERT, UPDATE, DELETE).
     * @param input     input parameters for the SQL statement.
     */
    public int executeUpdate(String procedure, Object... input) throws SQLException {
        txc.readOnly = false;
        // Replay.
        if (this.replayMode == ApiaryConfig.ReplayMode.SINGLE.getValue()) {
            replayUpdate(procedure, input);
            return 0;
        }

        int res = 0;
        if ((ApiaryConfig.captureMetadata && (this.workerContext.provBuff != null)) ||
                (replayMode == ApiaryConfig.ReplayMode.SELECTIVE.getValue())) {
            // Append the "RETURNING *" clause to the SQL query, so we can capture data updates.
            int querySeqNum = txc.querySeqNum.getAndIncrement();
            String interceptedQuery = interceptUpdate((String) procedure);
            ResultSet rs;
            ResultSetMetaData rsmd;
            String tableName;
            int exportOperation = Utilities.getQueryType(interceptedQuery);
            // First, prepare statement. Then, execute.
            currPstmt = conn.prepareStatement(interceptedQuery);
            prepareStatement(currPstmt, input);
            rs = currPstmt.executeQuery();
            rsmd = rs.getMetaData();
            tableName = rsmd.getTableName(1);

            // If it's a selective replay, then record tableName in the write set.
            if (replayMode == ApiaryConfig.ReplayMode.SELECTIVE.getValue()) {
                this.replayWrittenTables.add(tableName.toUpperCase());
                return 0;
            }
            long timestamp = Utilities.getMicroTimestamp();
            int numCol = rsmd.getColumnCount();
            // Record query metadata.
            Object[] metaData = new Object[5];
            metaData[0] = txc.txID;
            metaData[1] = querySeqNum;
            metaData[2] = currPstmt.toString();
            metaData[3] = tableName;
            metaData[4] = "*";
            workerContext.provBuff.addEntry(ProvenanceBuffer.PROV_QueryMetadata, metaData);

            // Record provenance data.
            if (ApiaryConfig.captureUpdates) {
                while (rs.next()) {
                    Object[] rowData = new Object[numCol + 4];
                    rowData[0] = txc.txID;
                    rowData[1] = timestamp;
                    rowData[2] = exportOperation;
                    rowData[3] = querySeqNum;
                    for (int i = 1; i <= numCol; i++) {
                        rowData[i + 3] = rs.getObject(i);
                    }
                    workerContext.provBuff.addEntry(tableName + "Events", rowData);
                }
            }
            rs.close();
            currPstmt.close();
        } else {
            // First, prepare statement. Then, execute.
            currPstmt = conn.prepareStatement(procedure);
            prepareStatement(currPstmt, input);
            res = currPstmt.executeUpdate();
            currPstmt.close();
        }
        return res;
    }

    /**
     * Execute bulk inserts into a table in a batch. Do not have provenance capture for this type of operation.
     * @param procedure a SQL DML statement (INSERT).
     * @param inputs     an array of input parameters for the SQL statement.
     */
    public void insertMany(String procedure, List<Object[]> inputs) throws SQLException {
        txc.readOnly = false;
        currPstmt = conn.prepareStatement(procedure);
        for (Object[] input : inputs) {
            prepareStatement(currPstmt, input);
            currPstmt.addBatch();
        }
        currPstmt.executeBatch();
        currPstmt.close();
    }

    /**
     * Execute a database query.
     * @param procedure a SQL query.
     * @param input     input parameters for the SQL statement.
     */
    public ResultSet executeQuery(String procedure, Object... input) throws SQLException {
        // Replay
        if (this.replayMode == ApiaryConfig.ReplayMode.SINGLE.getValue()) {
            return replayQuery(procedure, input);
        }
        currPstmt = conn.prepareStatement(procedure, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        if (input != null) {
            prepareStatement(currPstmt, input);
        }
        ResultSet rs = currPstmt.executeQuery();
        if ((workerContext.provBuff != null) && ApiaryConfig.captureMetadata) {
            int querySeqNum = txc.querySeqNum.getAndIncrement();
            Object[] metaData = new Object[5];
            metaData[0] = txc.txID;
            metaData[1] = querySeqNum;
            metaData[2] = currPstmt.toString();
            Set<String> tableNames = new HashSet<>();
            List<String> projection = new ArrayList<>();

            if (!ApiaryConfig.captureReads && (this.replayMode == ApiaryConfig.ReplayMode.NOT_REPLAY.getValue())) {
                // Only capture metadata.
                ResultSetMetaData rsmd = rs.getMetaData();
                int colCnt = rsmd.getColumnCount();
                for (int i = 1; i <= colCnt; i++) {
                    tableNames.add(rsmd.getTableName(i));
                    projection.add(rsmd.getColumnName(i));
                }
                metaData[3] = tableNames.stream()
                        .filter(s -> s != null && !s.isEmpty())
                        .collect(Collectors.joining(","));
                metaData[4] = projection.stream()
                        .filter(s -> s != null && !s.isEmpty())
                        .collect(Collectors.joining(","));
                // Only capture metadata.
                workerContext.provBuff.addEntry(ProvenanceBuffer.PROV_QueryMetadata, metaData);
            } else if (ApiaryConfig.captureReads) {
                long timestamp = Utilities.getMicroTimestamp();
                int exportOperation = Utilities.getQueryType(procedure);
                // Record provenance data.
                Map<String, Object[]> tableToRowData = new HashMap<>();
                if (!rs.next()) {
                    // Still need to record the table name and projection.
                    for (int colNum = 1; colNum <= rs.getMetaData().getColumnCount(); colNum++) {
                        String tableName = rs.getMetaData().getTableName(colNum);
                        if (!tableToRowData.containsKey(tableName)) {
                            tableToRowData.put(tableName, null);
                        }
                        projection.add(rs.getMetaData().getColumnName(colNum));
                    }
                    tableNames.addAll(tableToRowData.keySet());
                    tableToRowData.clear();
                } else {
                    do {
                        for (int colNum = 1; colNum <= rs.getMetaData().getColumnCount(); colNum++) {
                            String tableName = rs.getMetaData().getTableName(colNum);
                            Map<String, Integer> schemaMap = getSchemaMap(tableName);
                            if (!tableToRowData.containsKey(tableName)) {
                                Object[] rowData = new Object[4 + schemaMap.size()];
                                rowData[0] = txc.txID;
                                rowData[1] = timestamp;
                                rowData[2] = exportOperation;
                                rowData[3] = querySeqNum;
                                tableToRowData.put(tableName, rowData);
                            }
                            Object[] rowData = tableToRowData.get(tableName);
                            String columnName = rs.getMetaData().getColumnName(colNum);
                            if (schemaMap.containsKey(columnName)) {
                                int index = schemaMap.get(rs.getMetaData().getColumnName(colNum));
                                rowData[4 + index] = rs.getObject(colNum);
                            }
                            if (tableNames.isEmpty()) {
                                // Only add it once.
                                projection.add(columnName);
                            }
                        }
                        for (String tableName : tableToRowData.keySet()) {
                            workerContext.provBuff.addEntry(tableName + "Events", tableToRowData.get(tableName));
                        }
                        if (tableNames.isEmpty()) {
                            tableNames.addAll(tableToRowData.keySet());
                        }
                        tableToRowData.clear();
                    } while (rs.next());
                }
                // Record query metadata.
                // TODO: maybe find ways to parse SQL qeury and record metadata before execution.
                metaData[3] = tableNames.stream()
                        .filter(s -> s != null && !s.isEmpty())
                        .collect(Collectors.joining(","));
                metaData[4] = projection.stream()
                        .filter(s -> s != null && !s.isEmpty())
                        .collect(Collectors.joining(","));
                workerContext.provBuff.addEntry(ProvenanceBuffer.PROV_QueryMetadata, metaData);
                rs.beforeFirst();
            }
        }
        return rs;
    }

    /* --------------- For internal use ----------------- */

    private String interceptUpdate(String query) {
        // Remove the semicolon.
        String res = query.replace(';', ' ').toUpperCase(Locale.ROOT);
        res += " RETURNING *;";
        return res;
    }

    private static final Map<String, Map<String, Integer>> schemaMapCache = new ConcurrentHashMap<>();
    private Map<String, Integer> getSchemaMap(String tableName) throws SQLException {
        if (!schemaMapCache.containsKey(tableName)) {
            Map<String, Integer> schemaMap = new HashMap<>();
            ResultSet columns = conn.getMetaData().getColumns(null, null, tableName, null);
            int index = 0;
            while (columns.next()) {
                schemaMap.put(columns.getString("COLUMN_NAME"), index);
                index++;
            }
            schemaMapCache.put(tableName, schemaMap);
        }
        return schemaMapCache.get(tableName);
    }

    private void replayUpdate(String procedure, Object... input) throws SQLException {
        // TODO: support multiple replay modes. The current mode skips the insert during replay.
        int seqNum = this.txc.querySeqNum.getAndIncrement();
        logger.debug("Replay update. Original transaction: {} querySeqNum: {}",
                this.replayTxID, seqNum);

        String interceptedQuery = interceptUpdate(procedure);
        String currentQuery;
        String originalQuery;
        currPstmt = conn.prepareStatement(interceptedQuery);
        prepareStatement(currPstmt, input);
        currentQuery = currPstmt.toString();

        // Check the original query.
        currPstmt = conn.prepareStatement(checkMetadata);
        currPstmt.setLong(1, this.replayTxID);
        currPstmt.setLong(2, seqNum);
        ResultSet rs = currPstmt.executeQuery();
        if (rs.next()) {
            originalQuery = rs.getString(ProvenanceBuffer.PROV_QUERY_STRING);
            assert (currentQuery.equalsIgnoreCase(originalQuery));
            logger.debug("Replay original update: {}", originalQuery.split(" RETURNING")[0]);
        } else {
            throw new RuntimeException("Failed to find original update.");
        }
    }

    private ResultSet replayQuery(String procedure, Object... input) throws SQLException {
        // TODO: support multiple replay modes. The current mode queries the original provenance data and returns the results.
        int seqNum = this.txc.querySeqNum.getAndIncrement();
        logger.debug("Replay query. Original transaction: {} querySeqNum: {}",
                this.replayTxID, seqNum);

        currPstmt = conn.prepareStatement(procedure, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        if (input != null) {
            prepareStatement(currPstmt, input);
        }
        String currentQuery = currPstmt.toString();
        String originalQuery;
        List<String> tables;
        String projection;

        // Check the original query.
        currPstmt = conn.prepareStatement(checkMetadata, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        currPstmt.setLong(1, this.replayTxID);
        currPstmt.setLong(2, seqNum);
        ResultSet rs = currPstmt.executeQuery();
        if (rs.next()) {
            originalQuery = rs.getString(ProvenanceBuffer.PROV_QUERY_STRING);
            assert (currentQuery.equalsIgnoreCase(originalQuery));
            logger.debug("Replay original query: {}", originalQuery);
            String tableString = rs.getString(ProvenanceBuffer.PROV_QUERY_TABLENAMES);
            tables = List.of(tableString.split(","));
            projection = rs.getString(ProvenanceBuffer.PROV_QUERY_PROJECTION);
        } else {
            throw new RuntimeException("Failed to find original query.");
        }

        // Query the corresponding provenance table for replay.
        // TODO: for now, it does not support Joins. Only one table.
        if (tables.size() > 1) {
            throw new RuntimeException("Currently do not support more than one table in the query.");
        }
        String provQuery = String.format("SELECT %s FROM %sEvents WHERE %s=? AND %s=?", projection, tables.get(0), ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_QUERY_SEQNUM);
        currPstmt = conn.prepareStatement(provQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        currPstmt.setLong(1, this.replayTxID);
        currPstmt.setLong(2, seqNum);
        rs.close();
        rs = currPstmt.executeQuery();
        return rs;
    }
}
