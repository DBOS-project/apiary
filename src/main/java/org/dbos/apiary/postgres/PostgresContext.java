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
            ProvenanceBuffer.PROV_FuncInvocations, ProvenanceBuffer.PROV_EXECUTIONID, ProvenanceBuffer.PROV_FUNCID, ProvenanceBuffer.PROV_ISREPLAY);

    public TransactionContext txc;

    Map<String, Map<String, List<String>>> secondaryWrittenKeys = new HashMap<>();

    public PostgresContext(Connection c, WorkerContext workerContext, String service, long execID, long functionID,
                           boolean isReplay,
                           Set<TransactionContext> activeTransactions, Set<TransactionContext> abortedTransactions) {
        super(workerContext, service, execID, functionID, isReplay);
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

            // Look up the original transaction ID if it's a replay.
            if (isReplay) {
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
            return f.apiaryRunFunction(this, inputs);
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
            } else {
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
        int querySeqNum = txc.querySeqNum.getAndIncrement();
        // Replay.
        if (this.isReplay) {
            replayUpdate(procedure, input);
            return;
        }
        if (ApiaryConfig.captureUpdates && (this.workerContext.provBuff != null)) {
            // Append the "RETURNING *" clause to the SQL query, so we can capture data updates.
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
            if (!rs.next()) {
                // Still record an empty entry for the query.
                Object[] rowData = new Object[4];
                rowData[0] = txc.txID;
                rowData[1] = timestamp;
                rowData[2] = exportOperation;
                rowData[3] = querySeqNum;
                workerContext.provBuff.addEntry(tableName + "Events", rowData);
            } else {
                // Record each returned entry.
                do {
                    Object[] rowData = new Object[numCol + 4];
                    rowData[0] = txc.txID;
                    rowData[1] = timestamp;
                    rowData[2] = exportOperation;
                    rowData[3] = querySeqNum;
                    for (int i = 1; i <= numCol; i++) {
                        rowData[i + 3] = rs.getObject(i);
                    }
                    workerContext.provBuff.addEntry(tableName + "Events", rowData);
                } while (rs.next());
            }

        } else {
            // First, prepare statement. Then, execute.
            PreparedStatement pstmt = conn.prepareStatement(procedure);
            prepareStatement(pstmt, input);
            pstmt.executeUpdate();
        }
    }

    /**
     * Execute a database query.
     * @param procedure a SQL query.
     * @param input     input parameters for the SQL statement.
     */
    public ResultSet executeQuery(String procedure, Object... input) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(procedure, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        prepareStatement(pstmt, input);
        int querySeqNum = txc.querySeqNum.getAndIncrement();
        ResultSet rs = pstmt.executeQuery();
        // Replay
        if (this.isReplay) {
            // TODO: implement this part.
            rs.afterLast();
            return rs;
        }
        if (ApiaryConfig.captureReads && workerContext.provBuff != null) {
            long timestamp = Utilities.getMicroTimestamp();
            int exportOperation = Utilities.getQueryType(procedure);
            // Record provenance data.
            Map<String, Object[]> tableToRowData = new HashMap<>();
            List<String> tableNames = new ArrayList<>();
            List<String> projection = new ArrayList<>();
            if (!rs.next()) {
                // Still record the query itself even if it retrieved nothing.
                for (int colNum = 1; colNum <= rs.getMetaData().getColumnCount(); colNum++) {
                    String tableName = rs.getMetaData().getTableName(colNum);
                    if (!tableToRowData.containsKey(tableName)) {
                        Object[] rowData = new Object[4];
                        rowData[0] = txc.txID;
                        rowData[1] = timestamp;
                        rowData[2] = exportOperation;
                        rowData[3] = querySeqNum;
                        tableToRowData.put(tableName, rowData);
                    }
                    projection.add(rs.getMetaData().getColumnName(colNum));
                }
                for (String tableName : tableToRowData.keySet()) {
                    workerContext.provBuff.addEntry(tableName + "Events", tableToRowData.get(tableName));
                }
                if (tableNames.isEmpty()) {
                    tableNames.addAll(tableToRowData.keySet());
                }
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
            Object[] metaData = new Object[5];
            metaData[0] = txc.txID;
            metaData[1] = querySeqNum;
            metaData[2] = pstmt.toString();
            metaData[3] = String.join(",", tableNames);
            metaData[4] = String.join(",", projection);
            workerContext.provBuff.addEntry(ProvenanceBuffer.PROV_QueryMetadata, metaData);
            rs.beforeFirst();
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

    private void replayUpdate(String procedure, Object... input) {
        // TODO: support multiple replay modes. The current mode skips the insert during replay.
        int seqNum = this.txc.querySeqNum.get();
        logger.info("Replay update. Original transaction: {} querySeqNum: {}",
                this.replayTxID, seqNum);

        PreparedStatement pstmt = null;
        String interceptedQuery = interceptUpdate(procedure);
        String currentQuery;
        String originalQuery;
        try {
            pstmt = conn.prepareStatement(interceptedQuery);
            prepareStatement(pstmt, input);
            currentQuery = pstmt.toString();

            // Check the original query.
            String checkMetadata = String.format("SELECT %s FROM %s WHERE %s=? AND %s=?", ProvenanceBuffer.PROV_QUERY_STRING,
                    ProvenanceBuffer.PROV_ApiaryMetadata, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID, ProvenanceBuffer.PROV_QUERY_SEQNUM);
            pstmt = conn.prepareStatement(checkMetadata);
            pstmt.setLong(1, this.replayTxID);
            pstmt.setLong(2, seqNum);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                originalQuery = rs.getString(1);
                assert (currentQuery.equalsIgnoreCase(originalQuery));
                logger.info("Replay original update: {}", originalQuery);
            } else {
                throw new RuntimeException("Failed to find original update.");
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
}
