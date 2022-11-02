package org.dbos.apiary.postgres;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.ProvenanceBuffer;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.utilities.ApiaryConfig;
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
        this.ds = new PGSimpleDataSource();
        this.ds.setServerNames(new String[] {hostname});
        this.ds.setPortNumbers(new int[] {port});
        this.ds.setDatabaseName(ApiaryConfig.dbosDBName);  // Default use dbos databse.
        this.ds.setUser(databaseUsername);
        this.ds.setPassword(databasePassword);
        this.ds.setSsl(false);

        logger.info("Postgres isolation level: {}", ApiaryConfig.isolationLevel);
        this.connection = ThreadLocal.withInitial(() -> {
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
        });
        try {
            Connection testConn = ds.getConnection();
            testConn.close();
        } catch (SQLException e) {
            logger.info("Failed to connect to Postgres");
            throw new RuntimeException("Failed to connect to Postgres");
        }
        createTable(ProvenanceBuffer.PROV_FuncInvocations,
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID + " BIGINT NOT NULL, "
                + ProvenanceBuffer.PROV_APIARY_TIMESTAMP + " BIGINT NOT NULL, "
                + ProvenanceBuffer.PROV_EXECUTIONID + " BIGINT NOT NULL, "
                + ProvenanceBuffer.PROV_FUNCID + " BIGINT NOT NULL, "
                + ProvenanceBuffer.PROV_ISREPLAY + " SMALLINT NOT NULL, "
                + ProvenanceBuffer.PROV_SERVICE + " VARCHAR(1024) NOT NULL, "
                + ProvenanceBuffer.PROV_PROCEDURENAME + " VARCHAR(1024) NOT NULL");
        createTable(ProvenanceBuffer.PROV_ApiaryMetadata,
                "Key VARCHAR(1024) NOT NULL, Value Integer, PRIMARY KEY(key)");
        createTable(ProvenanceBuffer.PROV_QueryMetadata,
                ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID + " BIGINT NOT NULL, "
                + ProvenanceBuffer.PROV_QUERY_SEQNUM + " BIGINT NOT NULL, "
                + ProvenanceBuffer.PROV_QUERY_STRING + " VARCHAR(2048) NOT NULL, "
                + ProvenanceBuffer.PROV_QUERY_TABLENAMES + " VARCHAR(1024) NOT NULL, "
                + ProvenanceBuffer.PROV_QUERY_PROJECTION + " VARCHAR(1024) NOT NULL "
        );
        // TODO: add back recorded outputs later for fault tolerance.
        // createTable("RecordedOutputs", "ExecID bigint, FunctionID bigint, StringOutput VARCHAR(1000), IntOutput integer, StringArrayOutput bytea, IntArrayOutput bytea, FutureOutput bigint, QueuedTasks bytea, PRIMARY KEY(ExecID, FunctionID)");
    }

    /**
     * Drop a table and its corresponding events table if they exist.
     * @param tableName the table to drop.
     * @throws SQLException
     */
    public void dropTable(String tableName) throws SQLException {
        Connection conn = ds.getConnection();
        Statement truncateTable = conn.createStatement();
        truncateTable.execute(String.format("DROP TABLE IF EXISTS %s;", tableName));
        truncateTable.execute(String.format("DROP TABLE IF EXISTS %sEvents;", tableName));
        truncateTable.close();
        conn.close();
    }

    /**
     * Create a table and a corresponding events table.
     * @param tableName the table to create.
     * @param specStr the schema of the table, in Postgres DDL.
     * @throws SQLException
     */
    public void createTable(String tableName, String specStr) throws SQLException {
        Connection conn = ds.getConnection();
        Statement s = conn.createStatement();
        s.execute(String.format("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, specStr));
        if (!specStr.contains("APIARY_TRANSACTION_ID")) {
            ResultSet r = s.executeQuery(String.format("SELECT * FROM %s", tableName));
            ResultSetMetaData rsmd = r.getMetaData();
            StringBuilder provTable = new StringBuilder(String.format(
                    "CREATE TABLE IF NOT EXISTS %sEvents (%s BIGINT NOT NULL, %s BIGINT NOT NULL, %s BIGINT NOT NULL, %s BIGINT NOT NULL",
                    tableName, ProvenanceBuffer.PROV_APIARY_TRANSACTION_ID,
                    ProvenanceBuffer.PROV_APIARY_TIMESTAMP, ProvenanceBuffer.PROV_APIARY_OPERATION_TYPE,
                    ProvenanceBuffer.PROV_QUERY_SEQNUM));
            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                provTable.append(",");
                provTable.append(rsmd.getColumnLabel(i + 1));
                provTable.append(" ");
                provTable.append(rsmd.getColumnTypeName(i + 1));
            }
            provTable.append(");");
            s.execute(provTable.toString());
        }
        s.close();
        conn.close();
    }

    public void createIndex(String indexString) throws SQLException {
        Connection c = ds.getConnection();
        Statement s = c.createStatement();
        s.execute(indexString);
        s.close();
        c.close();
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
    public FunctionOutput callFunction(String functionName, WorkerContext workerContext, String service, long execID,
                                       long functionID, boolean isReplay, Object... inputs) {
        Connection c = connection.get();
        FunctionOutput f = null;
        while (true) {
            activeTransactionsLock.readLock().lock();
            PostgresContext ctxt = new PostgresContext(c, workerContext, service, execID, functionID, isReplay,
                    new HashSet<>(activeTransactions), new HashSet<>(abortedTransactions));
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
                    break;
                } else {
                    rollback(ctxt);
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
                        if (p.getSQLState().equals(PSQLState.SERIALIZATION_FAILURE.getState())) {
                            try {
                                rollback(ctxt);
                                continue;
                            } catch (SQLException ex) {
                                ex.printStackTrace();
                            }
                        } else {
                            logger.info("Unrecoverable Postgres error: {} {}", p.getMessage(), p.getSQLState());
                        }
                    }
                }
                logger.info("Unrecoverable error in function execution: {}", e.getMessage());
                e.printStackTrace();
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

}
