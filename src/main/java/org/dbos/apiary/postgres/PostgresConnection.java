package org.dbos.apiary.postgres;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.sqlite.SQLiteFunctionInterface;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class PostgresConnection implements ApiaryConnection {

    private final Connection c;
    private final Map<String, SQLiteFunctionInterface> functions = new HashMap<>();

    public PostgresConnection(Connection c) throws SQLException {
        this.c = c;
    }

    @Override
    public FunctionOutput callFunction(String name, long pkey, Object... inputs) throws Exception {
        StringBuilder input = new StringBuilder();
        String sep = "";
        for (Object o : inputs) {
            assert (o instanceof String);
            input.append(sep).append("'").append((String) o).append("'");
            sep = ",";
        }
        Statement s = c.createStatement();
        String query = "SELECT " + name + "(" + input + ");";
        ResultSet rs = s.executeQuery(query);
        rs.next();
        return new FunctionOutput(rs.getString(1), null, new ArrayList<>());
    }
}
