package org.dbos.apiary.mysql;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.mysql.cj.jdbc.exceptions.MySQLTransactionRollbackException;

import org.dbos.apiary.connection.ApiarySecondaryConnection;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.utilities.Percentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class MysqlConnection implements ApiarySecondaryConnection {
    private static final Logger logger = LoggerFactory.getLogger(MysqlConnection.class);
    
    public Percentile upserts = new Percentile();
    public Percentile queries = new Percentile();;
    public Percentile commits = new Percentile();;

    private final MysqlDataSource ds;
    private final ThreadLocal<Connection> connection;
    private final ThreadLocal<Connection> commitConnection;
    private boolean delayLogFlush = false;
    private final Map<String, Map<String, Set<Long>>> committedWrites = new ConcurrentHashMap<>();
    private final Lock validationLock = new ReentrantLock();

    public void enableDelayedFlush() {
        this.delayLogFlush = true;
        try {
            Connection conn = ds.getConnection();
            conn.setAutoCommit(true);
            Statement s = conn.createStatement();
            s.execute("SET GLOBAL innodb_flush_log_at_trx_commit=0;");
            s.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public MysqlConnection(String hostname, Integer port, String databaseName, String databaseUsername, String databasePassword) throws SQLException {
        this.ds = new MysqlDataSource();
        // Set dataSource Properties
        this.ds.setServerName(hostname);
        this.ds.setPortNumber(port);
        this.ds.setDatabaseName(databaseName);
        this.ds.setUser(databaseUsername);
        this.ds.setPassword(databasePassword);

        this.connection = ThreadLocal.withInitial(() -> {
            try {
                Connection conn = ds.getConnection();
                conn.setAutoCommit(true);
                // conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                return conn;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        });

        this.commitConnection = ThreadLocal.withInitial(() -> {
            try {
                Connection conn = ds.getConnection();
                conn.setAutoCommit(true);
                if (delayLogFlush) {
                    Statement s = conn.createStatement();
                    s.execute("SET GLOBAL innodb_flush_log_at_trx_commit=0;");
                    s.close();
                } else {
                    Statement s = conn.createStatement();
                    s.execute("SET GLOBAL innodb_flush_log_at_trx_commit=1;");
                    s.close();
                }
                // conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
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
            logger.info("Failed to connect to MySQL");
            throw new RuntimeException("Failed to connect to MySQL");
        }
    }

    public void dropTable(String tableName) throws SQLException {
        Connection conn = ds.getConnection();
        Statement truncateTable = conn.createStatement();
        truncateTable.execute(String.format("DROP TABLE IF EXISTS %s;", tableName));
        truncateTable.close();
        conn.close();
    }

    public void createTable(String tableName, String specStr) throws SQLException {
        // TODO: How do we interact with the original primary key columns to avoid conflicts? Add Apiary columns as part of primary key? For now, assume no primary key columns.
        // Automatically add three additional columns: apiaryID, beginVersion, endVersion.
        Connection conn = ds.getConnection();
        Statement s = conn.createStatement();
        String apiaryTable = String.format(
                "CREATE TABLE IF NOT EXISTS %s (%s VARCHAR(256) NOT NULL, %s BIGINT, %s BIGINT, %s);"
        , tableName, MysqlContext.apiaryID, MysqlContext.beginVersion, MysqlContext.endVersion, specStr);
        s.execute(apiaryTable);
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

    @Override
    public FunctionOutput callFunction(String functionName, Map<String, List<String>> writtenKeys, WorkerContext workerContext, TransactionContext txc, String service, long execID, long functionID, Object... inputs) throws Exception {
        MysqlContext ctxt = new MysqlContext(this.connection.get(), writtenKeys, workerContext, txc, service, execID, functionID, upserts, queries);
        FunctionOutput f = null;
        try {
            f = workerContext.getFunction(functionName).apiaryRunFunction(ctxt, inputs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return f;
    }

    @Override
    public void rollback(Map<String, List<String>> writtenKeys, TransactionContext txc) {
        String query = "";
        try {
            Connection c = ds.getConnection();
            Statement s = c.createStatement();
            for (String table : writtenKeys.keySet()) {
                query = String.format("DELETE FROM %s WHERE %s = %d", table, MysqlContext.beginVersion, txc.txID);
                s.execute(query);
            }
            s.close();
            c.close();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Failed to rollback txn {}", txc.txID);
            logger.info("Rollback query: {}", query);
        }
    }

    @Override
    public boolean validate(Map<String, List<String>> writtenKeys, TransactionContext txc) {
        long t0 = System.nanoTime();
        Set<Long> activeTransactions = new HashSet<>(txc.activeTransactions);
        validationLock.lock();
        boolean valid = true;
        for (String table: writtenKeys.keySet()) {
            for (String key : writtenKeys.get(table)) {
                // Has the key been modified by a transaction not in the snapshot?
                Set<Long> writes = committedWrites.getOrDefault(table, Collections.emptyMap()).getOrDefault(key, Collections.emptySet());
                for (Long write : writes) {
                    if (write >= txc.xmax || activeTransactions.contains(write)) {
                        valid = false;
                        break;
                    }
                }
            }
        }
        if (valid) {
            for (String collection: writtenKeys.keySet()) {
                for (String key : writtenKeys.get(collection)) {
                    committedWrites.putIfAbsent(collection, new ConcurrentHashMap<>());
                    committedWrites.get(collection).putIfAbsent(key, ConcurrentHashMap.newKeySet());
                    committedWrites.get(collection).get(key).add(txc.txID);
                }
            }
        }
        validationLock.unlock();
        if (valid) {
            String query = "";
            Connection c = null;
            Statement s = null;
            try {
                c = commitConnection.get();
                s = c.createStatement();
            } catch (Exception e) {
                e.printStackTrace();
            }
            while (true) {
                try {
                    s.clearBatch();
                    for (String table : writtenKeys.keySet()) {
                        String keyString = String.join(",", writtenKeys.get(table).stream().map(name -> ("'" + name + "'")).collect(Collectors.toList()));
                        if (!keyString.isEmpty()) {
                            query = String.format("UPDATE %s SET %s = %d WHERE %s IN (%s) AND %s < %d AND %s = %d", table, MysqlContext.endVersion, txc.txID, MysqlContext.apiaryID, keyString, MysqlContext.beginVersion, txc.txID, MysqlContext.endVersion, Long.MAX_VALUE);
                            //s.execute(query);
                            s.addBatch(query);
                        }
                    }
                    s.executeBatch();
                    if (delayLogFlush) {
                        s.execute("FLUSH ENGINE LOGS;");
                    }
                } catch(BatchUpdateException e) {
                    Throwable innerException = e.getCause();
                    if (innerException instanceof MySQLTransactionRollbackException) {
                        MySQLTransactionRollbackException m = (MySQLTransactionRollbackException)innerException;
                        if (m.getErrorCode() == 1213 || m.getErrorCode() == 1205) {
                            continue; // Deadlock or lock timed out
                        }
                    }
                    e.printStackTrace();
                    logger.error("1. Failed to update valid txn {}", txc.txID);
                    logger.info("1. Validate update query: {}", query);
                } catch (MySQLTransactionRollbackException m) {
                    if (m.getErrorCode() == 1213 || m.getErrorCode() == 1205) {
                        continue; // Deadlock or lock timed out
                    } else {
                        m.printStackTrace();
                        logger.error("2. Failed to update valid txn {}", txc.txID);
                        logger.info("2. Validate update query: {}", query);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("3. Failed to update valid txn {}", txc.txID);
                    logger.info("3. Validate update query: {}", query);
                }
                break;
            }
            try {
                s.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long time = System.nanoTime() - t0;
        commits.add(time / 1000);
        return valid;
    }

    @Override
    public void commit(Map<String, List<String>> writtenKeys, TransactionContext txc) {

    }

    @Override
    public void garbageCollect(Set<TransactionContext> activeTransactions) {
        long globalxmin = activeTransactions.stream().mapToLong(i -> i.xmin).min().getAsLong();
        // No need to keep track of writes that are visible to all active or future transactions.
        committedWrites.values().forEach(i -> i.values().forEach(w -> w.removeIf(txID -> txID < globalxmin)));
        // Delete old versions that are no longer visible to any active or future transaction.
        String query = "";
        try {
            Connection c = ds.getConnection();
            Statement s = c.createStatement();
            for (String tableName : committedWrites.keySet()) {
                query = String.format("DELETE FROM %s WHERE %s < %d", tableName, MysqlContext.endVersion, globalxmin);
                s.execute(query);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Failed to garbage collect: {}", query);
        }
    }
}
