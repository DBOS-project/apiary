package org.dbos.apiary.mysql;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.dbos.apiary.connection.ApiarySecondaryConnection;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MysqlConnection implements ApiarySecondaryConnection {
    private static final Logger logger = LoggerFactory.getLogger(MysqlConnection.class);

    private final MysqlDataSource ds;
    private final ThreadLocal<Connection> connection;

    private final Map<String, Map<String, Set<Long>>> committedWrites = new ConcurrentHashMap<>();
    private final Lock validationLock = new ReentrantLock();

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
        logger.info("Create table: {}", apiaryTable);
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
    public FunctionOutput callFunction(String functionName, WorkerContext workerContext, TransactionContext txc, String service, long execID, long functionID, Object... inputs) throws Exception {
        MysqlContext ctxt = new MysqlContext(this.connection.get(), workerContext, txc, service, execID, functionID);
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
            try {
                Connection c = ds.getConnection();
                Statement s = c.createStatement();
                for (String table : writtenKeys.keySet()) {
                    for (String key : writtenKeys.get(table)) {
                        query = String.format("UPDATE %s SET %s = %d WHERE %s = '%s' AND %s < %d AND %s = %d", table, MysqlContext.endVersion, txc.txID, MysqlContext.apiaryID, key, MysqlContext.beginVersion, txc.txID, MysqlContext.endVersion, Long.MAX_VALUE);
                        s.execute(query);
                    }
                }
                s.close();
                c.close();
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("Failed to update valid txn {}", txc.txID);
                logger.info("Validate update query: {}", query);
            }
        }
        return valid;
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
