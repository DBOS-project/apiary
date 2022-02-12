package org.dbos.apiary.sqlite;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.executor.FunctionOutput;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class SQLiteConnection implements ApiaryConnection {

    private final Connection c;
    private final Map<String, SQLiteFunctionInterface> functions = new HashMap<>();

    public SQLiteConnection(Connection c) throws SQLException {
        this.c = c;
        c.setAutoCommit(false);
    }

    public void createTable(String statement) throws SQLException {
        Statement s = c.createStatement();
        s.execute(statement);
        s.close();
    }

    public void registerFunction(String name, SQLiteFunctionInterface function) {
        functions.put(name, function);
    }

    @Override
    public FunctionOutput callFunction(String name, long pkey, Object... inputs) throws Exception {
        SQLiteFunctionInterface function = functions.get(name);
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
}
