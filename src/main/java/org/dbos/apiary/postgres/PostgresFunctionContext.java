package org.dbos.apiary.postgres;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.interposition.ApiaryFunction;
import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.interposition.ProvenanceBuffer;
import org.dbos.apiary.utilities.Utilities;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.Locale;

public class PostgresFunctionContext extends ApiaryStatefulFunctionContext {
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
            PreparedStatement pstmt = conn.prepareStatement((String) procedure);
            prepareStatement(pstmt, input);
            return pstmt.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected Object internalExecuteQueryCaptured(Object procedure, int[] primaryKeyCols, Object... input) {
        ResultSet rs = null;
        ResultSetMetaData rsmd;
        String tableName;
        String interceptedQuery = (String) procedure;
        int exportOperation = getQueryType(interceptedQuery);
        try {
            // First, prepare statement. Then, execute.
            PreparedStatement pstmt = conn.prepareStatement(interceptedQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            prepareStatement(pstmt, input);
            rs = pstmt.executeQuery();
            rsmd = rs.getMetaData();
            tableName = rsmd.getTableName(1);
            long timestamp = Utilities.getMicroTimestamp();
            // Record provenance data.
            Object[] rowData = new Object[primaryKeyCols.length+3];
            rowData[0] = this.transactionId;
            rowData[1] = timestamp;
            rowData[2] = exportOperation;
            while (rs.next()) {
                int colidx = 3;
                for (int primaryKeyCol : primaryKeyCols) {
                    rowData[colidx++] = rs.getObject(primaryKeyCol);
                }
                provBuff.addEntry(tableName, rowData);
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
}
