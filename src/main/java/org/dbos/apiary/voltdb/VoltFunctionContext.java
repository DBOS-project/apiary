package org.dbos.apiary.voltdb;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.interposition.ApiaryFunction;
import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.interposition.ProvenanceBuffer;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.voltdb.DeprecatedProcedureAPIAccess;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VoltFunctionContext extends ApiaryStatefulFunctionContext {

    private final VoltApiaryProcedure p;
    private long transactionID;

    public VoltFunctionContext(VoltApiaryProcedure p, ProvenanceBuffer provBuff, String service, long execID) {
        // TODO: add actual provenance buffer, service name, and execution ID.
        super(provBuff, service, execID);
        this.p = p;
        this.transactionID = internalGetTransactionId();
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
        return f.apiaryRunFunction(ctxt, inputs);
    }

    @Override
    protected void internalExecuteUpdate(Object procedure, Object... input) {
        p.voltQueueSQL((SQLStmt) procedure, input);
        p.voltExecuteSQL();
    }

    @Override
    protected void internalExecuteUpdateCaptured(Object procedure, Object... input) {
        String sqlStr = ((SQLStmt) procedure).getText();
        // TODO: currently only captures "INSERT INTO <table> VALUES (?,...)". Support more patterns later.
        long timestamp = Utilities.getMicroTimestamp();
        String tableName = getUpdateTableName(sqlStr);
        String upperName = tableName.toUpperCase();
        Object[] rowData = new Object[input.length+3];
        rowData[0] = this.transactionID;
        rowData[1] = timestamp;
        rowData[2] = Utilities.getQueryType(sqlStr);
        System.arraycopy(input, 0, rowData, 3, input.length);
        p.voltQueueSQL((SQLStmt) procedure, input);
        p.voltExecuteSQL();
        provBuff.addEntry(upperName, rowData);
    }

    @Override
    protected VoltTable[] internalExecuteQuery(Object procedure, Object... input) {
        p.voltQueueSQL((SQLStmt) procedure, input);
        return p.voltExecuteSQL();
    }

    @Override
    protected VoltTable[] internalExecuteQueryCaptured(Object procedure, Object... input) {
        // TODO: Volt doesn't differentiate columns returned from different tables. This capture won't capture the record if a query assigns aliases for columns.
        String sqlStr = ((SQLStmt) procedure).getText();
        String tableName = getSelectTableNames(sqlStr);
        p.voltQueueSQL((SQLStmt) procedure, input);
        VoltTable[] vs = p.voltExecuteSQL();
        VoltTable v = vs[0];
        long timestamp = Utilities.getMicroTimestamp();
        int queryType = ProvenanceBuffer.ExportOperation.READ.getValue();

        String upperTable = tableName.toUpperCase(Locale.ROOT);
        Map<String, Integer> localSchemaMap = getSchemaMap(upperTable);

        // Record provenance data.
        while (v.advanceRow()) {
            Object[] rowData = new Object[3 + localSchemaMap.size()];
            rowData[0] = this.transactionID;
            rowData[1] = timestamp;
            rowData[2] = queryType;
            for (int colNum = 0; colNum < v.getColumnCount(); colNum++) {
                String columnName = v.getColumnName(colNum);
                // Check if the table has this column name.
                if (localSchemaMap.containsKey(columnName)) {
                    int colIndex = localSchemaMap.get(columnName);
                    rowData[3 + colIndex] = v.get(colNum, v.getColumnType(colNum));
                }
            }
            provBuff.addEntry(tableName, rowData);
        }

        v.resetRowPosition();
        return vs;
    }

    @Override
    protected long internalGetTransactionId() {
        return DeprecatedProcedureAPIAccess.getVoltPrivateRealTransactionId(this.p);
    }

    // Borrow the idea from https://github.com/VoltDB/voltdb/blob/57bb02c61db33959efaefdc5f510ef44b170cad9/src/frontend/org/voltdb/NonVoltDBBackend.java#L628
    private static final Pattern UPDATE_TABLE_NAME = Pattern.compile(
            "(IN|UP)SERT\\s+INTO\\s+(?<table1>\\w+)",
            Pattern.CASE_INSENSITIVE);

    // Used below, to define SELECT_TABLE_NAMES
    private static final String TABLE_REFERENCE = "(?<table1>\\w+)(\\s+(AS\\s+)?\\w+)?";

    /** Pattern used to recognize the table names in a SELECT statement; will
     *  recognize up to 1 table names. */
    private static final Pattern SELECT_TABLE_NAMES = Pattern.compile(
            "(?<!DISTINCT)\\s+FROM\\s+(?<table1>\\w+)?\\s+",
            Pattern.CASE_INSENSITIVE);

    private String getUpdateTableName(String sqlStr) {
        String result = null;
        Matcher matcher = UPDATE_TABLE_NAME.matcher(sqlStr);
        if (matcher.find()) {
            String group = null;
            try {
                group = matcher.group("table1");
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
            assert (group != null);
            result = group;
        }
        return result;
    }

    private String getSelectTableNames(String sqlStr) {
        String result = null;
        Matcher matcher = SELECT_TABLE_NAMES.matcher(sqlStr);
        if (matcher.find()) {
            // TODO: capture all Join tables.
            String group = null;
            try {
                group = matcher.group("table1");
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
            assert (group != null);
            result = group;
        }
        return result;
    }

    private static final Client voltClient;
    private static final Map<String, Map<String, Integer>> schemaMapCache = new ConcurrentHashMap<>();

    static {
        ClientConfig config = new ClientConfig();
        voltClient = ClientFactory.createClient(config);
        try {
            voltClient.createConnection("localhost", ApiaryConfig.voltdbPort);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private Map<String, Integer> getSchemaMap(String tableName) {
        if (!schemaMapCache.containsKey(tableName)) {
            Map<String, Integer> schemaMap = new HashMap<>();
            VoltTable voltMap = null;
            try {
                voltMap = voltClient.callProcedure("@SystemCatalog", "columns").getResults()[0];
            } catch (IOException | ProcCallException e) {
                e.printStackTrace();
                return null;
            }
            while (voltMap.advanceRow()) {
                String table = voltMap.getString("TABLE_NAME");
                if (!table.equals(tableName)) {
                    continue;
                }
                String colName = voltMap.getString("COLUMN_NAME");
                // Index starts from 1.
                int colIdx = (int) voltMap.getLong("ORDINAL_POSITION") - 1;
                schemaMap.put(colName, colIdx);
            }
            schemaMapCache.put(tableName, schemaMap);
        }
        return schemaMapCache.get(tableName);
    }
}
