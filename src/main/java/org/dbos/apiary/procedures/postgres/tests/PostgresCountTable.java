package org.dbos.apiary.procedures.postgres.tests;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgresCountTable extends PostgresFunction {

    // Select count(*) from table;
    public static int runFunction(PostgresContext ctxt, String tableName) throws SQLException {
        String query = String.format("SELECT COUNT(*) FROM %s;", tableName);
        ResultSet r = ctxt.executeQuery(query);

        if (r.next()) {
            return r.getInt(1);
        }
        return -1;
    }
}
