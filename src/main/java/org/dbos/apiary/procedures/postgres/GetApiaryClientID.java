package org.dbos.apiary.procedures.postgres;

import org.dbos.apiary.function.ApiaryTransactionalContext;
import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class GetApiaryClientID extends PostgresFunction {

    private static final String get = "SELECT Value from ApiaryMetadata WHERE Key=?;";
    private static final String insert = "INSERT INTO ApiaryMetadata(Key, Value) VALUES (?, ?) ON CONFLICT (Key) DO UPDATE SET Value = EXCLUDED.Value;";

    private static final String clientIDName = "ClientID";

    public static int runFunction(PostgresContext ctxt) throws SQLException {
        ResultSet r = (ResultSet) ctxt.executeQuery(get, clientIDName);
        int value;
        if (r.next()) {
            value = r.getInt(1);
        } else {
            value = 0;
        }
        ctxt.executeUpdate(insert, clientIDName, value + 1);
        return value + 1;
    }
}