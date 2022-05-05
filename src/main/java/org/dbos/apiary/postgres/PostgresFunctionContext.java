package org.dbos.apiary.postgres;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.interposition.ApiaryFunction;
import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.interposition.ProvenanceBuffer;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class PostgresFunctionContext extends ApiaryStatefulFunctionContext {
    private static final Logger logger = LoggerFactory.getLogger(PostgresFunctionContext.class);
    // This connection ties to all prepared statements in one transaction.
    private final Connection conn;
    private long transactionId;  // This is the transaction ID of the main transaction. Postgres subtransaction IDs are invisible.

    public PostgresFunctionContext(Connection c, ProvenanceBuffer provBuff, String service, long execID) {
        super(provBuff, service, execID);
        this.conn= c;
        this.transactionId = -1;
    }

    @Override
    public FunctionOutput apiaryCallFunction(ApiaryFunctionContext ctxt, String name, Object... inputs) {
        Object clazz;
        try {
            clazz = Class.forName(name).getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
            return null;
        }
        assert(clazz instanceof ApiaryFunction);
        ApiaryFunction f = (ApiaryFunction) clazz;
        // Remember current txid.
        try {
            Savepoint s = conn.setSavepoint();
            try {
                FunctionOutput o = f.apiaryRunFunction(ctxt, inputs);
                conn.releaseSavepoint(s);
                return o;
            } catch (Exception e) {
                e.printStackTrace();
                conn.rollback(s);
                conn.releaseSavepoint(s);
                return null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
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

    @Override
    protected void internalExecuteUpdate(Object procedure, Object... input) {
        try {
            // First, prepare statement. Then, execute.
            PreparedStatement pstmt = conn.prepareStatement((String) procedure);
            prepareStatement(pstmt, input);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void internalExecuteUpdateCaptured(Object procedure, Object... input) {
        // Append the "RETURNING *" clause to the SQL query, so we can capture data updates.
        String interceptedQuery = interceptUpdate((String) procedure);
        ResultSet rs;
        ResultSetMetaData rsmd;
        String tableName;
        int exportOperation = getQueryType(interceptedQuery);
        try {
            // First, prepare statement. Then, execute.
            PreparedStatement pstmt = conn.prepareStatement(interceptedQuery);
            prepareStatement(pstmt, input);
            rs = pstmt.executeQuery();
            rsmd = rs.getMetaData();
            tableName = rsmd.getTableName(1);
            long timestamp = Utilities.getMicroTimestamp();
            int numCol = rsmd.getColumnCount();
            // Record provenance data.
            Object[] rowData = new Object[numCol+3];
            rowData[0] = this.transactionId;
            rowData[1] = timestamp;
            rowData[2] = exportOperation;
            while (rs.next()) {
                for (int i = 1; i <= numCol; i++) {
                    rowData[i+2] = rs.getObject(i);
                }
                provBuff.addEntry(tableName, rowData);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Object internalExecuteQuery(Object procedure, Object... input) {
        try {
            // First, prepare statement. Then, execute.
            PreparedStatement pstmt = conn.prepareStatement((String) procedure, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            prepareStatement(pstmt, input);
            return pstmt.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected Object internalExecuteQueryCaptured(Object procedure, Object... input) {
        ResultSet rs = null;
        String query = (String) procedure;
        try {
            rs = (ResultSet) internalExecuteQuery(procedure, input);
            long timestamp = Utilities.getMicroTimestamp();
            // Record provenance data.
            Map<String, Object[]> tableToRowData = new HashMap<>();
            while (rs.next()) {
                for (int colNum = 1; colNum <= rs.getMetaData().getColumnCount(); colNum++) {
                    String tableName = rs.getMetaData().getTableName(colNum);
                    Map<String, Integer> schemaMap = getSchemaMap(tableName);
                    if (!tableToRowData.containsKey(tableName)) {
                        Object[] rowData = new Object[3 + schemaMap.size()];
                        rowData[0] = this.transactionId;
                        rowData[1] = timestamp;
                        rowData[2] = getQueryType(query);
                        tableToRowData.put(tableName, rowData);
                    }
                    Object[] rowData = tableToRowData.get(tableName);
                    String columnName = rs.getMetaData().getColumnName(colNum);
                    if (schemaMap.containsKey(columnName)) {
                        int index = schemaMap.get(rs.getMetaData().getColumnName(colNum));
                        rowData[3 + index] = rs.getObject(colNum);
                    }
                }
                for (String tableName: tableToRowData.keySet()) {
                    provBuff.addEntry(tableName, tableToRowData.get(tableName));
                }
            }
            rs.beforeFirst();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }

    @Override
    public long internalGetTransactionId() {
        if (this.transactionId >= 0) {
            return this.transactionId;
        }
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select txid_current();");
            rs.next();
            this.transactionId = rs.getLong(1);
        } catch (SQLException e) {
            e.printStackTrace();
            return 0L;
        }
        return this.transactionId;
    }

    /* --------------- For internal use ----------------- */
    private String interceptUpdate(String query) {
        // Remove the semicolon.
        String res = query.replace(';', ' ').toUpperCase(Locale.ROOT);
        res += " RETURNING *;";
        return res;
    }

    private int getQueryType(String query) {
        int res;
        if (query.contains("INSERT")) {
            res = ProvenanceBuffer.ExportOperation.INSERT.getValue();
        } else if (query.contains("DELETE")) {
            res = ProvenanceBuffer.ExportOperation.DELETE.getValue();
        } else if (query.contains("UPDATE")) {
            res = ProvenanceBuffer.ExportOperation.UPDATE.getValue();
        } else {
            res = ProvenanceBuffer.ExportOperation.READ.getValue();
        }
        return res;
    }

    private static final Map<String, Map<String, Integer>> schemaMapCache = new HashMap<>();
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
