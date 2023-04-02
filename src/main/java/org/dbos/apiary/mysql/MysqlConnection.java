package org.dbos.apiary.mysql;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.mysql.cj.jdbc.exceptions.MySQLTransactionRollbackException;

import org.dbos.apiary.connection.ApiarySecondaryConnection;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Percentile;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MysqlConnection implements ApiarySecondaryConnection {
    private static final Logger logger = LoggerFactory.getLogger(MysqlConnection.class);
    
    public Percentile upserts = new Percentile();
    public Percentile queries = new Percentile();
    public Percentile commits = new Percentile();

    private final MysqlDataSource ds;
    private final ThreadLocal<Connection> connection;
    private final Map<String, Map<String, Set<Long>>> committedWrites = new ConcurrentHashMap<>();
    private final Lock validationLock = new ReentrantLock();

    private final Map<String, Map<String, AtomicBoolean>> lockManager = new ConcurrentHashMap<>();

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
                // Manually commit transaction after function execution.
                conn.setAutoCommit(false);
                // MySQL default level is repeatable read.
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
        Connection conn = ds.getConnection();
        Statement s = conn.createStatement();
        String apiaryTable;
        if (ApiaryConfig.XDBTransactions) {
            // Automatically add three additional columns: apiaryID, beginVersion, endVersion.
            // TODO: How do we interact with the original primary key columns to avoid conflicts? Add Apiary columns as part of primary key? For now, assume no primary key columns.
            apiaryTable = String.format(
                    "CREATE TABLE IF NOT EXISTS %s (%s VARCHAR(256) NOT NULL, %s BIGINT, %s BIGINT, %s);"
                    , tableName, MysqlContext.apiaryID, MysqlContext.beginVersion, MysqlContext.endVersion, specStr);
        } else {
            apiaryTable = String.format(
                    "CREATE TABLE IF NOT EXISTS %s (%s);", tableName, specStr);
        }
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
    public FunctionOutput callFunction(String functionName, Map<String, List<String>> writtenKeys, WorkerContext workerContext, TransactionContext txc, String role, long execID, long functionID, Object... inputs) throws Exception {
        FunctionOutput f = null;

        // Otherwise, need to retry until succeeded.
        while (true) {
            MysqlContext ctxt = new MysqlContext(this.connection.get(), writtenKeys, lockManager, workerContext, txc, role, execID, functionID, upserts, queries);
            try {
                f = workerContext.getFunction(functionName).apiaryRunFunction(ctxt, inputs);
                if (writtenKeys.containsKey(MysqlContext.committedToken)) {
                    // This is a hack: it means the function is invoked directly from the worker, bypassing Postgres. So we have to commit here.
                    // For XDB txns, we commit at validation step.
                    this.connection.get().commit();
                }
            } catch (MySQLTransactionRollbackException m) {
                if (m.getErrorCode() == 1213 || m.getErrorCode() == 1205) {
                    continue; // Deadlock or lock timed out, already rolled back by MySQL.
                } else {
                    m.printStackTrace();
                    throw new PSQLException("2. Failed to run MysqlFunction", PSQLState.SERIALIZATION_FAILURE);
                }
            } catch (Exception e) {
                throw new PSQLException("3. Failed to run MysqlFunction", PSQLState.SERIALIZATION_FAILURE);
            }
            break;
        }
        return f;
    }

    @Override
    public void rollback(Map<String, List<String>> writtenKeys, TransactionContext txc) {
        // If the connection has not committed, use the normal rollback.
        if (!writtenKeys.containsKey(MysqlContext.committedToken)) {
            try {
                this.connection.get().rollback();
            } catch (SQLException e) {
                logger.error("Rollback failed :/");
                throw new RuntimeException(e);
            }
            for (String table : writtenKeys.keySet()) {
                for (String key : writtenKeys.get(table)) {
                    lockManager.get(table).get(key).set(false);
                }
            }
            return;
        }

        String query = null;
        while (true) {
            try {
                Connection c = this.connection.get();
                Statement s = c.createStatement();
                for (String table : writtenKeys.keySet()) {
                    if (table.equals(MysqlContext.committedToken)) {
                        continue;  // Skip.
                    }
                    query = String.format("DELETE FROM %s WHERE %s = %d", table, MysqlContext.beginVersion, txc.txID);
                    s.executeUpdate(query);
                    query = String.format("UPDATE %s SET %s = %d where %s = %d", table, MysqlContext.endVersion, Long.MAX_VALUE, MysqlContext.endVersion, txc.txID);
                    s.executeUpdate(query);
                    for (String key : writtenKeys.get(table)) {
                        lockManager.get(table).get(key).set(false);
                    }
                }
                s.close();
                c.commit();
            } catch (MySQLTransactionRollbackException m) {
                if (m.getErrorCode() == 1213 || m.getErrorCode() == 1205) {
                    continue; // Deadlock or lock timed out
                } else {
                    m.printStackTrace();
                    logger.error("2. Failed to update valid txn {}", txc.txID);
                    logger.info("2. Rollback MySQL query: {}", query);
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("3. Failed to rollback mysql txn {}", txc.txID);
                logger.info("3. Rollback MySQL query: {}", query);
            }
            break;
        }
    }

    @Override
    public boolean validate(Map<String, List<String>> writtenKeys, TransactionContext txc) {
        long t0 = System.nanoTime();
        try {
            this.connection.get().commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        writtenKeys.putIfAbsent(MysqlContext.committedToken, new ArrayList<>());
        Set<Long> activeTransactions = new HashSet<>(txc.activeTransactions);
        validationLock.lock();
        boolean valid = true;
        for (String table: writtenKeys.keySet()) {
            if (table.equals(MysqlContext.committedToken)) {
                continue;  // Skip.
            }
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
                if (collection.equals(MysqlContext.committedToken)) {
                    continue;  // Skip.
                }
                for (String key : writtenKeys.get(collection)) {
                    committedWrites.putIfAbsent(collection, new ConcurrentHashMap<>());
                    committedWrites.get(collection).putIfAbsent(key, ConcurrentHashMap.newKeySet());
                    committedWrites.get(collection).get(key).add(txc.txID);
                }
            }
        }
        validationLock.unlock();
        long time = System.nanoTime() - t0;
        commits.add(time / 1000);
        return valid;
    }

    @Override
    public void commit(Map<String, List<String>> writtenKeys, TransactionContext txc) {
        if (!writtenKeys.containsKey(MysqlContext.committedToken)) {
            try {
                this.connection.get().commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            writtenKeys.putIfAbsent(MysqlContext.committedToken, new ArrayList<>());
        }
        for (String table : writtenKeys.keySet()) {
            if (table.equals(MysqlContext.committedToken)) {
                continue;  // Skip.
            }
            for (String key : writtenKeys.get(table)) {
                lockManager.get(table).get(key).set(false);
            }
        }
    }

    @Override
    public void garbageCollect(Set<TransactionContext> activeTransactions) {
        long globalxmin = activeTransactions.stream().mapToLong(i -> i.xmin).min().getAsLong();
        // No need to keep track of writes that are visible to all active or future transactions.
        committedWrites.values().forEach(i -> i.values().forEach(w -> w.removeIf(txID -> txID < globalxmin)));
        // Delete old versions that are no longer visible to any active or future transaction.
        // Retry until success.
        String query = "";
        while (true) {
            try {
                Connection c = this.connection.get();
                c.setAutoCommit(false);
                for (String tableName : lockManager.keySet()) {
                    query = String.format("DELETE FROM %s WHERE %s < ?", tableName, MysqlContext.endVersion);
                    PreparedStatement pstmt = c.prepareStatement(query);
                    pstmt.setLong(1, globalxmin);
                    pstmt.executeUpdate();
                    pstmt.close();
                }
                c.commit();
            } catch (MySQLTransactionRollbackException m) {
                if (m.getErrorCode() == 1213 || m.getErrorCode() == 1205) {
                    continue; // Deadlock or lock timed out
                } else {
                    m.printStackTrace();
                    logger.error("2. Failed to garbage collect query: {}", query);
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("3. Failed to garbage collect query: {}", query);
            }
            break;
        }
    }

}
