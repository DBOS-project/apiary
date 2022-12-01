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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * PostgresContext is a context for Apiary-Postgres functions.
 * It provides methods for accessing a Postgres database.
 */
public class PostgresContext extends ApiaryContext {
    private static final Logger logger = LoggerFactory.getLogger(PostgresContext.class);
    // This connection ties to all prepared statements in one transaction.
    final Connection conn;
    private AtomicLong functionIDCounter = new AtomicLong(0);
    private long currentID = functionID;

    private final long replayTxID;  // The replayed transaction ID.

    private static final String checkReplayTxID = String.format("SELECT %s FROM %s WHERE %s=? AND %s=? AND %s=0", ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID,
            ApiaryConfig.tableFuncInvocations, ProvenanceBuffer.PROV_EXECUTIONID, ProvenanceBuffer.PROV_FUNCID, ProvenanceBuffer.PROV_ISREPLAY);

    private static final String checkMetadata = String.format("SELECT * FROM %s WHERE %s=? AND %s=?", ProvenanceBuffer.PROV_QueryMetadata, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_QUERY_SEQNUM);

    public TransactionContext txc;

    Map<String, Map<String, List<String>>> secondaryWrittenKeys = new HashMap<>();

    public PostgresContext(Connection c, WorkerContext workerContext, String service, long execID, long functionID,
                           int replayMode,
                           Set<TransactionContext> activeTransactions, Set<TransactionContext> abortedTransactions) {
        super(workerContext, service, execID, functionID, replayMode);
        this.conn = c;
        long tmpReplayTxID = -1;
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select txid_current();");
            rs.next();
            long txID = rs.getLong(1);
            rs = stmt.executeQuery("select pg_current_snapshot();");
            rs.next();
            String snapshotString = rs.getString(1);
            long xmin = PostgresUtilities.parseXmin(snapshotString);
            long xmax = PostgresUtilities.parseXmax(snapshotString);
            List<Long> activeTxIDs = PostgresUtilities.parseActiveTransactions(snapshotString);
            activeTxIDs.addAll(abortedTransactions.stream().map(t -> t.txID).filter(t -> t < xmax).collect(Collectors.toList()));
            for (TransactionContext t: activeTransactions) {
                if (t.txID < xmax && !activeTxIDs.contains(t.txID)) {
                    rs = stmt.executeQuery("select txid_status(" + t.txID + ");");
                    rs.next();
                    if (rs.getString("txid_status").equals("aborted")) {
                        activeTxIDs.add(t.txID);
                    }
                }
            }
            this.txc = new TransactionContext(txID, xmin, xmax, activeTxIDs);

            // Look up the original transaction ID if it's a single replay.
            if (replayMode == ApiaryConfig.ReplayMode.SINGLE.getValue()) {
                PreparedStatement pstmt = conn.prepareStatement(checkReplayTxID);
                pstmt.setLong(1, execID);
                pstmt.setLong(2, functionID);
                rs = pstmt.executeQuery();
                if (rs.next()) {
                    tmpReplayTxID = rs.getLong(1);
                    logger.info("Current transaction {} is a replay of executionID: {}, functionID: {}, original tranasction ID: {}", txID, execID, functionID, tmpReplayTxID);
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
                workerContext.provBuff.addEntry(ApiaryConfig.tableFuncInvocations, txc.txID, startTime, execID, functionID, (short) replayMode, service, name, endTime, ProvenanceBuffer.PROV_STATUS_EMBEDDED);
            }
            return fo;
        } else {
            ApiarySecondaryConnection c = workerContext.getSecondaryConnection(functionType);
            long newID = ((this.functionID + calledFunctionID.incrementAndGet()) << 4);
            Map<String, List<String>> writtenKeys = new HashMap<>();
            try {
                FunctionOutput fo = c.callFunction(name, writtenKeys, workerContext, txc, service, execID, newID, inputs);
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
                logger.info("type {} for input {} not recognized ", o.toString(), i);
                assert (false); // TODO: More types.
            }
        }
    }

    /**
     * Execute a database update.
     * @param procedure a SQL DML statement (e.g., INSERT, UPDATE, DELETE).
     * @param input     input parameters for the SQL statement.
     */
    public void executeUpdate(String procedure, Object... input) throws SQLException {
        // Replay.
        if (this.replayMode == ApiaryConfig.ReplayMode.SINGLE.getValue()) {
            replayUpdate(procedure, input);
            return;
        }

        if (ApiaryConfig.captureUpdates && (this.workerContext.provBuff != null)) {
            // Append the "RETURNING *" clause to the SQL query, so we can capture data updates.
            int querySeqNum = txc.querySeqNum.getAndIncrement();
            String interceptedQuery = interceptUpdate((String) procedure);
            ResultSet rs;
            ResultSetMetaData rsmd;
            String tableName;
            int exportOperation = Utilities.getQueryType(interceptedQuery);
            // First, prepare statement. Then, execute.
            PreparedStatement pstmt = conn.prepareStatement(interceptedQuery);
            prepareStatement(pstmt, input);
            rs = pstmt.executeQuery();
            rsmd = rs.getMetaData();
            tableName = rsmd.getTableName(1);
            long timestamp = Utilities.getMicroTimestamp();
            int numCol = rsmd.getColumnCount();
            // Record query metadata.
            // TODO: maybe we should record metaata before query execution. So we know what happened even if the query failed.
            Object[] metaData = new Object[5];
            metaData[0] = txc.txID;
            metaData[1] = querySeqNum;
            metaData[2] = pstmt.toString();
            metaData[3] = tableName;
            metaData[4] = "*";
            workerContext.provBuff.addEntry(ProvenanceBuffer.PROV_QueryMetadata, metaData);

            // Record provenance data.
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
        } else {
            // First, prepare statement. Then, execute.
            PreparedStatement pstmt = conn.prepareStatement(procedure);
            prepareStatement(pstmt, input);
            pstmt.executeUpdate();
        }
    }

    /**
     * Execute bulk inserts into a table in a batch. Do not have provenance capture for this type of operation.
     * @param procedure a SQL DML statement (INSERT).
     * @param inputs     an array of input parameters for the SQL statement.
     */
    public void insertMany(String procedure, List<Object[]> inputs) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(procedure);
        for (Object[] input : inputs) {
            prepareStatement(pstmt, input);
            pstmt.addBatch();
        }
        pstmt.executeBatch();
        pstmt.close();
        return;
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
        PreparedStatement pstmt = conn.prepareStatement(procedure, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        if (input != null) {
            prepareStatement(pstmt, input);
        }
        ResultSet rs = pstmt.executeQuery();
        if (workerContext.provBuff != null) {
            int querySeqNum = txc.querySeqNum.getAndIncrement();
            Object[] metaData = new Object[5];
            metaData[0] = txc.txID;
            metaData[1] = querySeqNum;
            metaData[2] = pstmt.toString();
            if (!ApiaryConfig.captureReads) {
                metaData[3] = "N/A";
                metaData[4] = "N/A";
                // Only capture metadata.
                workerContext.provBuff.addEntry(ProvenanceBuffer.PROV_QueryMetadata, metaData);
            } else {
                long timestamp = Utilities.getMicroTimestamp();
                int exportOperation = Utilities.getQueryType(procedure);
                // Record provenance data.
                Map<String, Object[]> tableToRowData = new HashMap<>();
                List<String> tableNames = new ArrayList<>();
                List<String> projection = new ArrayList<>();
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
                metaData[3] = String.join(",", tableNames);
                metaData[4] = String.join(",", projection);
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
        logger.info("Replay update. Original transaction: {} querySeqNum: {}",
                this.replayTxID, seqNum);

        PreparedStatement pstmt;
        String interceptedQuery = interceptUpdate(procedure);
        String currentQuery;
        String originalQuery;
        pstmt = conn.prepareStatement(interceptedQuery);
        prepareStatement(pstmt, input);
        currentQuery = pstmt.toString();

        // Check the original query.
        pstmt = conn.prepareStatement(checkMetadata);
        pstmt.setLong(1, this.replayTxID);
        pstmt.setLong(2, seqNum);
        ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
            originalQuery = rs.getString(ProvenanceBuffer.PROV_QUERY_STRING);
            assert (currentQuery.equalsIgnoreCase(originalQuery));
            logger.info("Replay original update: {}", originalQuery.split(" RETURNING")[0]);
        } else {
            throw new RuntimeException("Failed to find original update.");
        }
    }

    private ResultSet replayQuery(String procedure, Object... input) throws SQLException {
        // TODO: support multiple replay modes. The current mode queries the original provenance data and returns the results.
        int seqNum = this.txc.querySeqNum.getAndIncrement();
        logger.info("Replay query. Original transaction: {} querySeqNum: {}",
                this.replayTxID, seqNum);

        PreparedStatement pstmt = conn.prepareStatement(procedure, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        if (input != null) {
            prepareStatement(pstmt, input);
        }
        String currentQuery = pstmt.toString();
        String originalQuery;
        List<String> tables;
        String projection;

        // Check the original query.
        pstmt = conn.prepareStatement(checkMetadata, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        pstmt.setLong(1, this.replayTxID);
        pstmt.setLong(2, seqNum);
        ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
            originalQuery = rs.getString(ProvenanceBuffer.PROV_QUERY_STRING);
            assert (currentQuery.equalsIgnoreCase(originalQuery));
            logger.info("Replay original query: {}", originalQuery);
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
        pstmt = conn.prepareStatement(provQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        pstmt.setLong(1, this.replayTxID);
        pstmt.setLong(2, seqNum);
        rs.close();
        rs = pstmt.executeQuery();
        return rs;
    }
}
