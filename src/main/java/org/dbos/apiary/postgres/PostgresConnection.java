package org.dbos.apiary.postgres;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class PostgresConnection implements ApiaryConnection {
    private static final Logger logger = LoggerFactory.getLogger(PostgresConnection.class);

    private final PGSimpleDataSource ds;
    private final ThreadLocal<Connection> connection;

    private final Map<String, Callable<PostgresFunction>> functions = new HashMap<>();

    public PostgresConnection(String hostname, Integer port) {
        this.ds = new PGSimpleDataSource();
        this.ds.setServerNames(new String[] {hostname});
        this.ds.setPortNumbers(new int[] {port});
        this.ds.setDatabaseName("postgres");
        this.ds.setUser("postgres");
        this.ds.setPassword("dbos");
        this.ds.setSsl(false);

        this.connection = ThreadLocal.withInitial(() -> {
           try {
               Connection conn = ds.getConnection();
               conn.setAutoCommit(false);
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
    }

    public void registerFunction(String name, Callable<PostgresFunction> function) { functions.put(name, function); }

    public void dropTable(String tableName) throws SQLException {
        Connection conn = ds.getConnection();
        Statement truncateTable = conn.createStatement();
        truncateTable.execute(String.format("DROP TABLE IF EXISTS %s;", tableName));
        truncateTable.close();
        conn.close();
    }

    public void createTable(String tableName, String specStr) throws SQLException {
        Connection conn = ds.getConnection();
        Statement createTable = conn.createStatement();
        createTable.execute(String.format("CREATE TABLE IF NOT EXISTS %s %s;", tableName, specStr));
        createTable.close();
        conn.close();
    }

    @Override
    public FunctionOutput callFunction(String name, Object... inputs) throws Exception {
        PostgresFunction function = functions.get(name).call();
        ApiaryFunctionContext ctxt = new PostgresFunctionContext(connection.get());
        FunctionOutput f = null;
        try {
            f = function.apiaryRunFunction(ctxt, inputs);
            connection.get().commit();
        } catch (Exception e) {
            e.printStackTrace();
            connection.get().rollback();
        }
        return f;
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
