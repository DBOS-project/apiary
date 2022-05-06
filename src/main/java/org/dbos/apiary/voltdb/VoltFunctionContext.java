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
        Object[] rowData = new Object[input.length+3];
        rowData[0] = this.transactionID;
        rowData[1] = timestamp;
        rowData[2] = Utilities.getQueryType(sqlStr);
        System.arraycopy(input, 0, rowData, 3, input.length);
        p.voltQueueSQL((SQLStmt) procedure, input);
        p.voltExecuteSQL();
        provBuff.addEntry(tableName, rowData);
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
        List<String> tableNames = getSelectTableNames(sqlStr);
        p.voltQueueSQL((SQLStmt) procedure, input);
        VoltTable[] vs = p.voltExecuteSQL();
        VoltTable v = vs[0];
        long timestamp = Utilities.getMicroTimestamp();
        int queryType = ProvenanceBuffer.ExportOperation.READ.getValue();

        Map<String, Map<String, Integer>> localSchemaMap = new HashMap<>();
        for (String table : tableNames) {
            String upperTable = table.toUpperCase(Locale.ROOT);
            localSchemaMap.put(upperTable, getSchemaMap(upperTable));
        }

        // Record provenance data.
        Map<String, Object[]> tableToRowData = new HashMap<>();
        while (v.advanceRow()) {
            for (int colNum = 0; colNum < v.getColumnCount(); colNum++) {
                String columnName = v.getColumnName(colNum);
                // Check which table has this column name.
                Map<String, Integer> currSchemaMap = null;
                String currTable = null;
                for (String table : localSchemaMap.keySet()) {
                    if (localSchemaMap.get(table).containsKey(columnName)) {
                        currSchemaMap = localSchemaMap.get(table);
                        currTable = table;
                    }
                }
                if (currSchemaMap != null) {
                    if (!tableToRowData.containsKey(currTable)) {
                        Object[] rowData = new Object[3 + currSchemaMap.size()];
                        rowData[0] = this.transactionID;
                        rowData[1] = timestamp;
                        rowData[2] = queryType;
                        tableToRowData.put(currTable, rowData);
                    }
                    Object[] rowData = tableToRowData.get(currTable);
                    int colIndex = currSchemaMap.get(columnName);
                    rowData[3 + colIndex] = v.get(colNum, v.getColumnType(colNum));
                }
            }
            for (String tableName: tableToRowData.keySet()) {
                provBuff.addEntry(tableName, tableToRowData.get(tableName));
            }
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
     *  recognize up to 2 table names. */
    private static final Pattern SELECT_TABLE_NAMES = Pattern.compile(
            "(?<!DISTINCT)\\s+FROM\\s+(?<table1>\\w+)?\\s+(((INNER|CROSS|((LEFT|RIGHT|FULL)\\s+)?OUTER)\\s+)?JOIN\\s+(?<table2>\\w+)?\\s+)?",
            Pattern.CASE_INSENSITIVE);
    private static final int MAX_NUM_TABLES = 2;

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

    private List<String> getSelectTableNames(String sqlStr) {
        List<String> result = new ArrayList<>();
        Matcher matcher = SELECT_TABLE_NAMES.matcher(sqlStr);
        if (matcher.find()) {
            // TODO: capture all Join tables.
            String group;
            for (int i = 1; i <= MAX_NUM_TABLES; i++) {
                try {
                    group = matcher.group("table"+i);
                } catch (IllegalArgumentException e) {
                    break;
                }
                if (group != null) {
                    result.add(group);
                }
            }
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
