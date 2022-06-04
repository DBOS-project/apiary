package org.dbos.apiary.postgres;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.function.*;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PostgresContext is a context for Apiary-Postgres functions.
 * It provides methods for accessing a Postgres database.
 */
public class PostgresContext extends ApiaryContext {
    private static final Logger logger = LoggerFactory.getLogger(PostgresContext.class);
    // This connection ties to all prepared statements in one transaction.
    private final Connection conn;
    private AtomicLong functionIDCounter = new AtomicLong(0);
    private long currentID = functionID;

    public long transactionId;
    private long xmax;
    private long xmin;
    private long[] activeTransactions;

    public PostgresContext(Connection c, WorkerContext workerContext, String service, long execID, long functionID) {
        super(workerContext, service, execID, functionID);
        this.conn = c;
        this.transactionId = -1;

        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select txid_current();");
            rs.next();
            this.transactionId = rs.getLong(1);
            rs = stmt.executeQuery("select  pg_current_snapshot();");
            rs.next();
            String snapshotString = rs.getString(1);
            this.xmax = PostgresUtilities.parseXmax(snapshotString);
            this.xmin = PostgresUtilities.parseXmin(snapshotString);
            this.activeTransactions = PostgresUtilities.parseActiveTransactions(snapshotString);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) {
        ApiaryFunction f = workerContext.getFunction(name);
        String functionType = workerContext.getFunctionType(name);
        if (functionType.equals(ApiaryConfig.postgres) || functionType.equals(ApiaryConfig.stateless)) {
            try {
                Savepoint s = conn.setSavepoint();
                long oldID = currentID;
                try {
                    this.currentID = functionID + functionIDCounter.incrementAndGet();
                    FunctionOutput o = f.apiaryRunFunction(this, inputs);
                    this.currentID = oldID;
                    conn.releaseSavepoint(s);
                    return o;
                } catch (Exception e) {
                    e.printStackTrace();
                    this.currentID = oldID;
                    conn.rollback(s);
                    conn.releaseSavepoint(s);
                    return null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        } else {
            try {
                ApiaryConnection c = workerContext.getConnection(functionType);
                long newID = ((this.functionID + calledFunctionID.incrementAndGet()) << 4);
                return c.callFunction(name, workerContext, service, execID, newID, inputs);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    @Override
    public FunctionOutput checkPreviousExecution() {
        try {
            Statement s = conn.createStatement();
            ResultSet r = s.executeQuery(String.format("SELECT * FROM RecordedOutputs WHERE ExecID=%d AND FunctionID=%d", execID, currentID));
            if (r.next()) {
                List<Task> queuedTasks;
                Object o;
                o = r.getBytes(8);
                if (!r.wasNull()) {
                    queuedTasks = List.of((Task[]) Utilities.byteArrayToObject((byte[]) o));
                } else {
                    queuedTasks = new ArrayList<>();
                }

                o = r.getString(3);
                if (!r.wasNull()) {
                    return new FunctionOutput(o, queuedTasks);
                }
                o = r.getInt(4);
                if (!r.wasNull()) {
                    return new FunctionOutput(o, queuedTasks);
                }
                o = r.getBytes(5);
                if (!r.wasNull()) {
                    return new FunctionOutput(Utilities.byteArrayToStringArray((byte[]) o), queuedTasks);
                }
                o = r.getBytes(6);
                if (!r.wasNull()) {
                    return new FunctionOutput(Utilities.byteArrayToIntArray((byte[]) o), queuedTasks);
                }
                o = r.getLong(7);
                if (!r.wasNull()) {
                    return new FunctionOutput(new ApiaryFuture((long) o), queuedTasks);
                }
                assert(false);
            }
            return null;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void recordExecution(FunctionOutput output) {
        try {
            PreparedStatement s = conn.prepareStatement("INSERT INTO RecordedOutputs(ExecID, FunctionID, StringOutput, IntOutput, StringArrayOutput, IntArrayOutput, FutureOutput, QueuedTasks) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
            s.setLong(1, execID);
            s.setLong(2, currentID);
            if (output.getString() != null) {
                s.setString(3, output.getString());
                s.setNull(4, Types.INTEGER);
                s.setNull(5, Types.VARBINARY);
                s.setNull(6, Types.VARBINARY);
                s.setNull(7, Types.INTEGER);
            } else if (output.getInt() != null) {
                s.setNull(3, Types.VARCHAR);
                s.setInt(4, output.getInt());
                s.setNull(5, Types.VARBINARY);
                s.setNull(6, Types.VARBINARY);
                s.setNull(7, Types.INTEGER);
            } else if (output.getStringArray() != null) {
                s.setNull(3, Types.VARCHAR);
                s.setNull(4, Types.INTEGER);
                s.setBytes(5, Utilities.stringArraytoByteArray(output.getStringArray()));
                s.setNull(6, Types.VARBINARY);
                s.setNull(7, Types.INTEGER);
            } else if (output.getIntArray() != null) {
                s.setNull(3, Types.VARCHAR);
                s.setNull(4, Types.INTEGER);
                s.setNull(5, Types.VARBINARY);
                s.setBytes(6, Utilities.intArrayToByteArray(output.getIntArray()));
                s.setNull(7, Types.INTEGER);
            }else if (output.getFuture() != null) {
                s.setNull(3, Types.VARCHAR);
                s.setNull(4, Types.INTEGER);
                s.setNull(5, Types.VARBINARY);
                s.setNull(6, Types.VARBINARY);
                s.setLong(7, output.getFuture().futureID);
            }
            if (!output.queuedTasks.isEmpty()) {
                s.setBytes(8, Utilities.objectToByteArray(output.queuedTasks.toArray(new Task[0])));
            } else {
                s.setNull(8, Types.VARBINARY);
            }
            s.executeUpdate();
            s.close();
        } catch (SQLException e) {
            logger.info("Execution record failed: {}", e.getSQLState());
            e.printStackTrace();
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
    public void executeUpdate(String procedure, Object... input) {
        if (ApiaryConfig.captureUpdates && (this.workerContext.provBuff != null)) {
            // Append the "RETURNING *" clause to the SQL query, so we can capture data updates.
            String interceptedQuery = interceptUpdate((String) procedure);
            ResultSet rs;
            ResultSetMetaData rsmd;
            String tableName;
            int exportOperation = Utilities.getQueryType(interceptedQuery);
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
                rowData[0] = transactionId;
                rowData[1] = timestamp;
                rowData[2] = exportOperation;
                while (rs.next()) {
                    for (int i = 1; i <= numCol; i++) {
                        rowData[i+2] = rs.getObject(i);
                    }
                    workerContext.provBuff.addEntry(tableName + "Events", rowData);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            try {
                // First, prepare statement. Then, execute.
                PreparedStatement pstmt = conn.prepareStatement(procedure);
                prepareStatement(pstmt, input);
                pstmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Execute a database query.
     * @param procedure a SQL query.
     * @param input     input parameters for the SQL statement.
     */
    public ResultSet executeQuery(String procedure, Object... input) {
        try {
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
                            rowData[0] = transactionId;
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
                }
                rs.beforeFirst();
            }
            return rs;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
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
