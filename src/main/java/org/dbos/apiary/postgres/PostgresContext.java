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

    public TransactionContext txc;

    Map<String, Map<String, List<String>>> secondaryWrittenKeys = new HashMap<>();

    public PostgresContext(Connection c, WorkerContext workerContext, String service, long execID, long functionID,
                           Set<TransactionContext> activeTransactions, Set<TransactionContext> abortedTransactions) {
        super(workerContext, service, execID, functionID);
        this.conn = c;
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
        } catch (Exception e) {
            e.printStackTrace();
        }
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
            FunctionOutput fo = c.callFunction(name, workerContext, txc, service, execID, newID, inputs);
            secondaryWrittenKeys.putIfAbsent(functionType, new HashMap<>());
            for (String table: fo.getWrittenKeys().keySet()) {
                secondaryWrittenKeys.get(functionType).putIfAbsent(table, new ArrayList<>());
                secondaryWrittenKeys.get(functionType).get(table).addAll(fo.getWrittenKeys().get(table));
            }
            return fo;
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
            // Record provenance data.
            while (rs.next()) {
                Object[] rowData = new Object[numCol+3];
                rowData[0] = txc.txID;
                rowData[1] = timestamp;
                rowData[2] = exportOperation;
                for (int i = 1; i <= numCol; i++) {
                    rowData[i+2] = rs.getObject(i);
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
     * Execute a database query.
     * @param procedure a SQL query.
     * @param input     input parameters for the SQL statement.
     */
    public ResultSet executeQuery(String procedure, Object... input) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(procedure, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        prepareStatement(pstmt, input);
        ResultSet rs = pstmt.executeQuery();
        if (ApiaryConfig.captureReads && workerContext.provBuff != null) {
            long timestamp = Utilities.getMicroTimestamp();
            // Record provenance data.
            Map<String, Object[]> tableToRowData = new HashMap<>();
            while (rs.next()) {
                for (int colNum = 1; colNum <= rs.getMetaData().getColumnCount(); colNum++) {
                    String tableName = rs.getMetaData().getTableName(colNum);
                    Map<String, Integer> schemaMap = getSchemaMap(tableName);
                    if (!tableToRowData.containsKey(tableName)) {
                        Object[] rowData = new Object[3 + schemaMap.size()];
                        rowData[0] = txc.txID;
                        rowData[1] = timestamp;
                        rowData[2] = Utilities.getQueryType(procedure);
                        tableToRowData.put(tableName, rowData);
                    }
                    Object[] rowData = tableToRowData.get(tableName);
                    String columnName = rs.getMetaData().getColumnName(colNum);
                    if (schemaMap.containsKey(columnName)) {
                        int index = schemaMap.get(rs.getMetaData().getColumnName(colNum));
                        rowData[3 + index] = rs.getObject(colNum);
                    }
                }
                for (String tableName : tableToRowData.keySet()) {
                    workerContext.provBuff.addEntry(tableName + "Events", tableToRowData.get(tableName));
                }
                tableToRowData.clear();
            }
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
}
