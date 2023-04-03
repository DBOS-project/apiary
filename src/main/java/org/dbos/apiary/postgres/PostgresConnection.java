package org.dbos.apiary.postgres;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.function.*;
import org.dbos.apiary.benchmarks.tpcc.UserAbortException;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A connection to a Postgres database.
 */
public class PostgresConnection implements ApiaryConnection {
    private static final Logger logger = LoggerFactory.getLogger(PostgresConnection.class);

    private final PGSimpleDataSource ds;
    public final ThreadLocal<Connection> connection;
    public final ThreadLocal<Connection> bgConnection;  // For background tasks, not the critical one for function executions.
    public final ThreadLocal<Connection> provConnection;  // Connect to the provenance database.
    private final ReadWriteLock activeTransactionsLock = new ReentrantReadWriteLock();
    private long biggestxmin = Long.MIN_VALUE;
    private final Set<TransactionContext> activeTransactions = ConcurrentHashMap.newKeySet();
    private final Set<TransactionContext> abortedTransactions = ConcurrentHashMap.newKeySet();
    private TransactionContext latestTransactionContext;

    /**
     * Create a connection to a Postgres database.
     *
     * @param hostname         the Postgres database hostname.
     * @param port             the Postgres database port.
     * @param databaseUsername the Postgres database username.
     * @param databasePassword the Postgres database password.
     * @throws SQLException
     */
    public PostgresConnection(String hostname, Integer port, String databaseUsername, String databasePassword) throws SQLException {
        // If no provenance database provided, use itself.
        this(hostname, port, databaseUsername, databasePassword, ApiaryConfig.postgres, hostname);
    }

    // With provenance.
    public PostgresConnection(String hostname, Integer port, String databaseUsername, String databasePassword, String provDBType, String provAddress) throws SQLException {
        this.ds = new PGSimpleDataSource();
        this.ds.setServerNames(new String[] {hostname});
        this.ds.setPortNumbers(new int[] {port});
        this.ds.setDatabaseName(ApiaryConfig.dbosDBName);  // Default use dbos databse.
        this.ds.setUser(databaseUsername);
        this.ds.setPassword(databasePassword);
        this.ds.setSsl(false);

        logger.debug("Postgres isolation level: {}", ApiaryConfig.isolationLevel);
        this.connection = ThreadLocal.withInitial(() -> createNewConnection());
        this.bgConnection = ThreadLocal.withInitial(() -> {
            try {
                Connection conn = ds.getConnection();
                conn.setAutoCommit(true);
                return conn;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        });
        try {
            Connection testConn = ds.getConnection();
            Statement stmt = testConn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW track_commit_timestamp;");
            rs.next();
            if (rs.getString(1).equals("on")) {
                ApiaryConfig.trackCommitTimestamp = true;
                logger.debug("Postgres track_commit_timestamp = on!");
            } else {
                ApiaryConfig.trackCommitTimestamp = false;
                logger.debug("Postgres track_commit_timestamp = off!");
            }
            testConn.close();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("Failed to connect to Postgres");
            throw new RuntimeException("Failed to connect to Postgres");
        }

        this.provConnection = ThreadLocal.withInitial(() -> {
           Connection conn = ProvenanceBuffer.createProvConnection(provDBType, provAddress);
            try {
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return conn;
        });

        createTable(this.bgConnection.get(), ProvenanceBuffer.PROV_ApiaryMetadata,
                "Key VARCHAR(1024) NOT NULL, Value Integer, PRIMARY KEY(key)");

        Connection provConn = provConnection.get();
        if (provConn == null) {
            logger.error("Failed to connect to provenance DB.");
            throw new RuntimeException("Failed to connect to provenance DB");
        }
        createTable(provConn, ApiaryConfig.tableFuncInvocations,
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID + " BIGINT NOT NULL, "
                        + ProvenanceBuffer.PROV_APIARY_TIMESTAMP + " BIGINT NOT NULL, "
                        + ProvenanceBuffer.PROV_EXECUTIONID + " BIGINT NOT NULL, "
                        + ProvenanceBuffer.PROV_FUNCID + " BIGINT NOT NULL, "
                        + ProvenanceBuffer.PROV_ISREPLAY + " BIGINT NOT NULL, "
                        + ProvenanceBuffer.PROV_APIARY_ROLE + " VARCHAR(256) NOT NULL, "
                        + ProvenanceBuffer.PROV_PROCEDURENAME + " VARCHAR(512) NOT NULL, "
                        + ProvenanceBuffer.PROV_END_TIMESTAMP + " BIGINT, "
                        + ProvenanceBuffer.PROV_FUNC_STATUS + " VARCHAR(20), "
                        + ProvenanceBuffer.PROV_TXN_SNAPSHOT + " VARCHAR(65000), "
                        + ProvenanceBuffer.PROV_READONLY + " BOOLEAN ");
        if (ApiaryConfig.captureMetadata) {
            createTable(provConn, ProvenanceBuffer.PROV_QueryMetadata,
                    ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID + " BIGINT NOT NULL, "
                            + ProvenanceBuffer.PROV_QUERY_SEQNUM + " BIGINT NOT NULL, "
                            + ProvenanceBuffer.PROV_QUERY_STRING + " VARCHAR(2048) NOT NULL, "
                            + ProvenanceBuffer.PROV_QUERY_TABLENAMES + " VARCHAR(1024) NOT NULL, "
                            + ProvenanceBuffer.PROV_QUERY_PROJECTION + " VARCHAR(1024) NOT NULL "
            );
        }

        if (ApiaryConfig.recordInput) {
            // Record input for replay. Only need to record the input of the first function, so we only need to use execID to find the arguments.
            if (provDBType.equals(ApiaryConfig.postgres)) {
                createTable(provConn, ApiaryConfig.tableRecordedInputs,
                        ProvenanceBuffer.PROV_EXECUTIONID + " BIGINT NOT NULL PRIMARY KEY, " +
                                ProvenanceBuffer.PROV_REQ_BYTES + " BYTEA NOT NULL");
            } else {
                createTable(provConn, ApiaryConfig.tableRecordedInputs,
                        ProvenanceBuffer.PROV_EXECUTIONID + " BIGINT NOT NULL PRIMARY KEY, " +
                                ProvenanceBuffer.PROV_REQ_BYTES + " VARBINARY(65000) NOT NULL");
            }
        }

        // TODO: add back recorded outputs later for fault tolerance.
        // createTable("RecordedOutputs", "ExecID bigint, FunctionID bigint, StringOutput VARCHAR(1000), IntOutput integer, StringArrayOutput bytea, IntArrayOutput bytea, FutureOutput bigint, QueuedTasks bytea, PRIMARY KEY(ExecID, FunctionID)");
    }

    /**
     * Drop a table and its corresponding events table if they exist.
     * @param tableName the table to drop.
     * @throws SQLException
     */
    public void dropTable(String tableName) throws SQLException {
        Connection conn = bgConnection.get();
        dropTable(conn, tableName);

        Connection provConn = provConnection.get();
        dropTable(provConn, tableName + "Events");
    }

    static public void dropTable(Connection conn, String tableName) throws SQLException {
        Statement dropTable = conn.createStatement();
        dropTable.execute(String.format("DROP TABLE IF EXISTS %s;", tableName));
        dropTable.close();
    }

    /**
     * Truncate a table and potentially its corresponding events table.
     * @param tableName         the table to truncate.
     * @param deleteProvenance  if true, truncate the events table as well.
     */
    public void truncateTable(String tableName, boolean deleteProvenance) throws SQLException {
        Connection conn = bgConnection.get();
        Statement truncateTable = conn.createStatement();
        truncateTable.execute(String.format("TRUNCATE %s;", tableName));
        truncateTable.close();
        if (deleteProvenance) {
            Connection provConn = provConnection.get();
            truncateTable = provConn.createStatement();
            truncateTable.execute(String.format("TRUNCATE %sEvents;", tableName));
            truncateTable.close();
        }
    }

    /**
     * Create a table and a corresponding events table.
     * @param tableName the table to create.
     * @param specStr the schema of the table, in Postgres DDL.
     * @throws SQLException
     */
    public void createTable(String tableName, String specStr) throws SQLException {
        Connection conn = bgConnection.get();
        createTable(conn, tableName, specStr);

        Connection provConn = provConnection.get();
        if (!specStr.contains(ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID)) {
            Statement s = conn.createStatement();
            ResultSet r = s.executeQuery(String.format("SELECT * FROM %s", tableName));
            ResultSetMetaData rsmd = r.getMetaData();
            StringBuilder provTable = new StringBuilder(String.format(
                    "%s BIGINT NOT NULL, %s BIGINT NOT NULL, %s BIGINT NOT NULL, %s BIGINT NOT NULL ",
                    ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID,
                    ProvenanceBuffer.PROV_APIARY_TIMESTAMP, ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE,
                    ProvenanceBuffer.PROV_QUERY_SEQNUM));
            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                provTable.append(",");
                provTable.append(rsmd.getColumnLabel(i + 1));
                provTable.append(" ");
                provTable.append(rsmd.getColumnTypeName(i + 1));
                if (rsmd.getColumnTypeName(i+1).equalsIgnoreCase("varchar")) {
                    provTable.append("(10000)");  // TODO: hack.
                }
            }
            logger.info("Table {} schema: {}", tableName + "Events", provTable.toString());
            createTable(provConn, tableName + "Events", provTable.toString());
            r.close();
            s.close();
        }
    }

    static private void createTable(Connection conn, String tableName, String specStr) throws SQLException {
        Statement s = conn.createStatement();
        s.execute(String.format("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, specStr));
        s.close();
    }

    public void createIndex(String indexString) throws SQLException {
        Connection c = bgConnection.get();
        Statement s = c.createStatement();
        s.execute(indexString);
        s.close();
    }

    private void rollback(PostgresContext ctxt) throws SQLException {
        abortedTransactions.add(ctxt.txc);
        for (String secondary : ctxt.secondaryWrittenKeys.keySet()) {
            Map<String, List<String>> updatedKeys = ctxt.secondaryWrittenKeys.get(secondary);
            ctxt.workerContext.getSecondaryConnection(secondary).rollback(updatedKeys, ctxt.txc);
        }
        ctxt.conn.rollback();
        abortedTransactions.remove(ctxt.txc);
        activeTransactions.remove(ctxt.txc);
    }

    @Override
    public Connection createNewConnection() {
        try {
            Connection conn = ds.getConnection();
            conn.setAutoCommit(false);
            if (ApiaryConfig.isolationLevel == ApiaryConfig.REPEATABLE_READ) {
                conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            } else if (ApiaryConfig.isolationLevel == ApiaryConfig.SERIALIZABLE) {
                conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            } else {
                logger.info("Invalid isolation level: {}", ApiaryConfig.isolationLevel);
            }
            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public FunctionOutput callFunction(String functionName, WorkerContext workerContext, String role, long execID,
                                       long functionID, int replayMode, Object... inputs) {
        Connection c = connection.get();
        FunctionOutput f = null;
        long tStart = System.nanoTime();
        while (true) {
            // Record invocation for each try, if we have provenance buffer.
            long startTime = Utilities.getMicroTimestamp();
            activeTransactionsLock.readLock().lock();
            PostgresContext ctxt = new PostgresContext(c, workerContext, role, execID, functionID, replayMode,
                    new HashSet<>(activeTransactions), new HashSet<>(abortedTransactions), new HashSet<>());
            activeTransactions.add(ctxt.txc);
            latestTransactionContext = ctxt.txc;
            if (ctxt.txc.xmin > biggestxmin) {
                biggestxmin = ctxt.txc.xmin;
            }
            activeTransactionsLock.readLock().unlock();
            try {
                f = workerContext.getFunction(functionName).apiaryRunFunction(ctxt, inputs);
                boolean valid = true;
                for (String secondary : ctxt.secondaryWrittenKeys.keySet()) {
                    Map<String, List<String>> writtenKeys = ctxt.secondaryWrittenKeys.get(secondary);
                    if (!writtenKeys.isEmpty()) {
                        valid &= ctxt.workerContext.getSecondaryConnection(secondary).validate(writtenKeys, ctxt.txc);
                    }
                }
                if (valid) {
                    ctxt.conn.commit();
                    for (String secondary : ctxt.secondaryWrittenKeys.keySet()) {
                        Map<String, List<String>> writtenKeys = ctxt.secondaryWrittenKeys.get(secondary);
                        ctxt.workerContext.getSecondaryConnection(secondary).commit(writtenKeys, ctxt.txc);
                    }
                    activeTransactions.remove(ctxt.txc);
                    // Record invocation information.
                    recordTransactionInfo(workerContext, ctxt, startTime, functionName, ProvenanceBuffer.PROV_STATUS_COMMIT);
                    break;
                } else {
                    rollback(ctxt);
                    recordTransactionInfo(workerContext, ctxt, startTime, functionName, ProvenanceBuffer.PROV_STATUS_FAIL_RECOVERABLE);
                }
            } catch (Exception e) {
                if (e instanceof InvocationTargetException) {
                    Throwable innerException = e;
                    while (innerException instanceof InvocationTargetException) {
                        InvocationTargetException i = (InvocationTargetException) innerException;
                        innerException = i.getCause();
                    }
                    if (innerException instanceof PSQLException) {
                        PSQLException p = (PSQLException) innerException;
                        f = new FunctionOutput(p.getMessage());
                        try {
                            rollback(ctxt);
                        } catch (SQLException ex) {
                            ex.printStackTrace();
                        }
                        if (p.getSQLState().equals(PSQLState.SERIALIZATION_FAILURE.getState())) {
                            recordTransactionInfo(workerContext, ctxt, startTime, functionName, ProvenanceBuffer.PROV_STATUS_FAIL_RECOVERABLE);
                            continue;  // Retry.
                        } else {
                            // Abort and return.
                            logger.error("Unrecoverable inner PSQLException error: {}, SQLState: {}", p.getMessage(), p.getSQLState());
                            recordTransactionInfo(workerContext, ctxt, startTime, functionName, ProvenanceBuffer.PROV_STATUS_FAIL_UNRECOVERABLE);
                            break;
                        }
                    } else if (innerException instanceof UserAbortException) {
                        // Abort and return.
                        UserAbortException p = (UserAbortException) innerException;
                        f = new FunctionOutput(p.getMessage());
                        try {
                            rollback(ctxt);
                        } catch (SQLException ex) {
                            ex.printStackTrace();
                        }
                        recordTransactionInfo(workerContext, ctxt, startTime, functionName, ProvenanceBuffer.PROV_STATUS_FAIL_UNRECOVERABLE);
                        break;
                    } else {
                        e.printStackTrace();
                        logger.error("Unrecoverable InvocationTargetException: {}", e.getMessage());
                      recordTransactionInfo(workerContext, ctxt, startTime, functionName, ProvenanceBuffer.PROV_STATUS_FAIL_UNRECOVERABLE);
                    }
                    break;
                } else if (e instanceof PSQLException) {
                    PSQLException p = (PSQLException) e;

                    f = new FunctionOutput(p.getMessage());
                    try {
                        rollback(ctxt);
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                    if (p.getSQLState().equals(PSQLState.SERIALIZATION_FAILURE.getState())) {
                        recordTransactionInfo(workerContext, ctxt, startTime, functionName, ProvenanceBuffer.PROV_STATUS_FAIL_RECOVERABLE);
                        continue;  // Retry.
                    } else {
                        // Abort and return.
                        logger.error("Unrecoverable top-level PSQLException error: {}, SQLState: {}", p.getMessage(), p.getSQLState());
                        recordTransactionInfo(workerContext, ctxt, startTime, functionName, ProvenanceBuffer.PROV_STATUS_FAIL_UNRECOVERABLE);
                        break;
                    }
                }
                logger.error("Unrecoverable error in function execution: {}", e.getMessage());
                e.printStackTrace();
                f = new FunctionOutput(e.getMessage());
                recordTransactionInfo(workerContext, ctxt, startTime, functionName, ProvenanceBuffer.PROV_STATUS_FAIL_UNRECOVERABLE);
                break;
            }
        }
        return f;
    }

    @Override
    public FunctionOutput replayFunction(ApiaryContext apCtxt, String functionName, Set<String> replayWrittenTables,
                                         Object... inputs) {
        PostgresContext pgCtxt = (PostgresContext) apCtxt;
        // Fast path for replayed functions.
        FunctionOutput f;
        String actualName = functionName;
        String replayStatus = ProvenanceBuffer.PROV_STATUS_REPLAY;

        PostgresContext ctxt = pgCtxt;
        while (true) {
            long startTime = Utilities.getMicroTimestamp();
            if (ctxt == null) {
                // During retry, create a new one.
                ctxt = new PostgresContext(pgCtxt.conn, pgCtxt.workerContext, pgCtxt.role, pgCtxt.execID, pgCtxt.functionID, pgCtxt.replayMode,
                        new HashSet<>(), new HashSet<>(), new HashSet<>());
            }

            try {
                ApiaryFunction func = ctxt.workerContext.getFunction(functionName);
                actualName = Utilities.getFunctionClassName(func);
                logger.debug("Replaying function [{}], inputs {}", actualName, inputs);
                f = func.apiaryRunFunction(ctxt, inputs);
                logger.debug("Completed function [{}]", actualName);
                // If it is read-only, commit now.
                if (ctxt.workerContext.getFunctionReadOnly(functionName)) {
                    ctxt.conn.commit();
                } else {
                    // Collect all written tables.
                    replayWrittenTables.addAll(ctxt.replayWrittenTables);
                }
                recordTransactionInfo(ctxt.workerContext, ctxt, startTime, actualName, replayStatus);

                break;
            } catch (Exception e) {
                try {
                    if (!ctxt.conn.isClosed()) {
                        rollback(ctxt);
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
                String errorMsg = e.getMessage();
                if (e instanceof InvocationTargetException) {
                    Throwable innerException = e;
                    while (innerException instanceof InvocationTargetException) {
                        InvocationTargetException i = (InvocationTargetException) innerException;
                        innerException = i.getCause();
                    }
                    if (innerException instanceof PSQLException) {
                        PSQLException p = (PSQLException) innerException;
                        errorMsg = p.getMessage();
                        // Only retry under retro mode. We should not have serialization error under faithful replay.
                        if (p.getSQLState().equals(PSQLState.SERIALIZATION_FAILURE.getState()) && ctxt.workerContext.hasRetroFunctions()) {
                            recordTransactionInfo(ctxt.workerContext, ctxt, startTime, actualName, ProvenanceBuffer.PROV_STATUS_FAIL_RECOVERABLE);
                            logger.debug("Serialization failure during replay execution, will retry: {}", errorMsg);

                            ctxt = null;  // Create a new one during retry.
                            continue;  // Retry.
                        }
                    }
                }

                if (e instanceof PSQLException) {
                    PSQLException p = (PSQLException) e;
                    errorMsg = p.getMessage();
                    // Only retry under retro mode. We should not have serialization error under faithful replay.
                    if (p.getSQLState().equals(PSQLState.SERIALIZATION_FAILURE.getState()) && ctxt.workerContext.hasRetroFunctions()) {
                        recordTransactionInfo(ctxt.workerContext, ctxt, startTime, actualName, ProvenanceBuffer.PROV_STATUS_FAIL_RECOVERABLE);
                        logger.debug("Serialization failure during replay commit, will retry: {}", errorMsg);

                        ctxt = null;  // Create a new one during retry.
                        continue;  // Retry.
                    }
                }
                logger.error("Unrecoverable failed execution during replay. Error: {}", errorMsg);
                replayStatus = ProvenanceBuffer.PROV_STATUS_FAIL_UNRECOVERABLE;
                f = new FunctionOutput(errorMsg);
                recordTransactionInfo(ctxt.workerContext, ctxt, startTime, actualName, replayStatus);
                break;
            }
        }
        return f;
    }

    @Override
    public Set<TransactionContext> getActiveTransactions() {
        activeTransactionsLock.writeLock().lock();
        Set<TransactionContext> txSnapshot = new HashSet<>(activeTransactions);
        if (txSnapshot.isEmpty()) {
            txSnapshot.add(new TransactionContext(0, biggestxmin, biggestxmin, new ArrayList<>()));
        }
        activeTransactionsLock.writeLock().unlock();
        return txSnapshot;
    }

    @Override
    public TransactionContext getLatestTransactionContext() {
        return latestTransactionContext;
    }

    @Override
    public void updatePartitionInfo() {
        // Nothing here.
        return;
    }

    @Override
    public int getNumPartitions() {
        return 1;
    }

    @Override
    public String getHostname(Object... input) {
        return "localhost";
    }

    @Override
    public Map<Integer, String> getPartitionHostMap() {
        return Map.of(0, "localhost");
    }

    private void recordTransactionInfo(WorkerContext workerContext, PostgresContext ctxt, long startTime, String functionName, String status) {
        if ((workerContext.provBuff == null) || (ctxt.execID == 0) || !ApiaryConfig.captureFuncInvocations) {
            return;
        }
        // Get actual commit timestamp if track_commit_timestamp is available. Otherwise, get the timestamp from Java.
        long commitTime = Utilities.getMicroTimestamp();
        String txnSnapshot = PostgresUtilities.constuctSnapshotStr(ctxt.txc.xmin, ctxt.txc.xmax, ctxt.txc.activeTransactions);
        workerContext.provBuff.addEntry(ApiaryConfig.tableFuncInvocations, ctxt.txc.txID, startTime, ctxt.execID, ctxt.functionID, (short)ctxt.replayMode, ctxt.role, functionName, commitTime, status, txnSnapshot, ctxt.txc.readOnly);
    }
}
