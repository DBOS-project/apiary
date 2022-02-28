package org.dbos.apiary.sqlite;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.executor.FunctionOutput;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class SQLiteConnection implements ApiaryConnection {

    private final Connection c;
    private final Map<String, Callable<SQLiteFunctionInterface>> functions = new HashMap<>();

    public SQLiteConnection(Connection c) throws SQLException {
        this.c = c;
        c.setAutoCommit(false);
    }

    public void createTable(String statement) throws SQLException {
        Statement s = c.createStatement();
        s.execute(statement);
        s.close();
    }

    public void registerFunction(String name, Callable<SQLiteFunctionInterface> function) {
        functions.put(name, function);
    }

    @Override
    public FunctionOutput callFunction(String name, Object... inputs) throws Exception {
        SQLiteFunctionInterface function = functions.get(name).call();
        FunctionOutput f = null;
        try {
            f = function.runFunction(inputs);
            c.commit();
        } catch (Exception e) {
            e.printStackTrace();
            c.rollback();
        }
        return f;
    }

    @Override
    public void updatePartitionInfo() { return; }

    @Override
    public int getNumPartitions() {
        return 1;
    }

    @Override
    public String getHostname(Object[] input) {
        return "localhost";
    }

    @Override
    public Map<Integer, String> getPartitionHostMap() {
        return null;
    }

}
