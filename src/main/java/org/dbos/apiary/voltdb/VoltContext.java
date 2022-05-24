package org.dbos.apiary.voltdb;

import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.Task;
import org.dbos.apiary.function.*;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.dbos.apiary.voltdb.VoltFunction.getRecordedOutput;
import static org.dbos.apiary.voltdb.VoltFunction.recordOutput;

/**
 * For internal use only.
 */
public class VoltContext extends ApiaryTransactionalContext {

    private final VoltFunction p;
    private long transactionID;
    private AtomicLong functionIDCounter = new AtomicLong(0);
    private long currentID = functionID;

    public VoltContext(VoltFunction p, ProvenanceBuffer provBuff, String service, long execID, long functionID) {
        super(provBuff, service, execID, functionID);
        this.p = p;
        this.transactionID = internalGetTransactionId();
    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) {
        Object clazz;
        try {
            clazz = Class.forName(name).getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
            return null;
        }
        assert(clazz instanceof ApiaryFunction);
        ApiaryFunction f = (ApiaryFunction) clazz;
        long oldID = currentID;
        this.currentID = functionID + functionIDCounter.incrementAndGet();
        FunctionOutput fo = f.apiaryRunFunction(this, inputs);
        this.currentID = oldID;
        return fo;
    }

    @Override
    public FunctionOutput checkPreviousExecution() {
        p.voltQueueSQL(getRecordedOutput, this.execID, this.currentID);
        VoltTable v = p.voltExecuteSQL()[0];
        if (v.getRowCount() > 0) {
            v.advanceRow();
            Object o;
            List<Task> queuedTasks;

            o = v.getVarbinary(8);
            if (!v.wasNull()) {
                queuedTasks = List.of((Task[])Utilities.byteArrayToObject((byte[]) o));
            } else {
                queuedTasks = new ArrayList<>();
            }

            o = v.getString(3);
            if (!v.wasNull()) {
                return new FunctionOutput(o, queuedTasks);
            }
            o = (int) v.getLong(4);
            if (!v.wasNull()) {
                return new FunctionOutput(o, queuedTasks);
            }
            o = v.getVarbinary(5);
            if (!v.wasNull()) {
                return new FunctionOutput(Utilities.byteArrayToStringArray((byte[]) o), queuedTasks);
            }
            o = v.getVarbinary(6);
            if (!v.wasNull()) {
                return new FunctionOutput(Utilities.byteArrayToIntArray((byte[]) o), queuedTasks);
            }
            o = v.getLong(7);
            if (!v.wasNull()) {
                return new FunctionOutput(new ApiaryFuture((long) o), queuedTasks);
            }
            assert (false);
        }
        return null;
    }

    @Override
    public void recordExecution(FunctionOutput output) {
        int pkey = this.p.pkey;
        long execID = this.execID;
        long functionID = this.currentID;
        String stringOutput = null;
        Integer intOutput = null;
        byte[] stringArrayOutput = null;
        byte[] intArrayOutput = null;
        Long futureOutput = null;
        byte[] queueuedTasks = null;
        if (output.getString() != null) {
            stringOutput = output.getString();
        } else if (output.getInt() != null) {
            intOutput = output.getInt();
        } else if (output.getStringArray() != null) {
            stringArrayOutput = Utilities.stringArraytoByteArray(output.getStringArray());
        } else if (output.getIntArray() != null) {
            intArrayOutput = Utilities.intArrayToByteArray(output.getIntArray());
        } else if (output.getFuture() != null) {
            futureOutput = output.getFuture().futureID;
        }
        if (!output.queuedTasks.isEmpty()) {
            queueuedTasks = Utilities.objectToByteArray(output.queuedTasks.toArray(new Task[0]));
        }
        p.voltQueueSQL(recordOutput, pkey, execID, functionID, stringOutput, intOutput, stringArrayOutput, intArrayOutput, futureOutput, queueuedTasks);
        p.voltExecuteSQL();
    }

    public void executeUpdate(SQLStmt procedure, Object... input) {
        if (ApiaryConfig.captureUpdates && (this.provBuff != null)) {
            // TODO: currently only captures "INSERT INTO <table> VALUES (?,...)". Support more patterns later.
            long timestamp = Utilities.getMicroTimestamp();
            String tableName = getUpdateTableName(procedure.getText());
            String upperName = tableName.toUpperCase();
            Object[] rowData = new Object[input.length+3];
            rowData[0] = this.transactionID;
            rowData[1] = timestamp;
            rowData[2] = Utilities.getQueryType(procedure.getText());
            System.arraycopy(input, 0, rowData, 3, input.length);
            p.voltQueueSQL(procedure, input);
            p.voltExecuteSQL();
            provBuff.addEntry(upperName + "EVENTS", rowData);
        } else {
            p.voltQueueSQL(procedure, input);
            p.voltExecuteSQL();
        }
    }

    public VoltTable[] executeQuery(Object procedure, Object... input) {
        if (ApiaryConfig.captureReads && (this.provBuff != null)) {
            // TODO: Volt doesn't differentiate columns returned from different tables.
            // TODO: This capture won't capture the record if a query assigns aliases for columns.
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
                provBuff.addEntry(tableName + "EVENTS", rowData);
            }
            v.resetRowPosition();
            return vs;
        } else {
            p.voltQueueSQL((SQLStmt) procedure, input);
            return p.voltExecuteSQL();
        }
    }

    @Override
    protected long internalGetTransactionId() {
        return DeprecatedProcedureAPIAccess.getVoltPrivateRealTransactionId(this.p);
    }

    // Borrow the idea from https://github.com/VoltDB/voltdb/blob/57bb02c61db33959efaefdc5f510ef44b170cad9/src/frontend/org/voltdb/NonVoltDBBackend.java#L628
    private static final Pattern UPDATE_TABLE_NAME = Pattern.compile(
            "(IN|UP)SERT\\s+INTO\\s+(?<table1>\\w+)",
            Pattern.CASE_INSENSITIVE);

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
