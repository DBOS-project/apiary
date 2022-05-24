package org.dbos.apiary.procedures.postgres.tests;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgresIncrementFunction extends PostgresFunction {

    private static final String get = "SELECT KVValue from KVTable WHERE KVKey=?;";
    private static final String insert = "INSERT INTO KVTable(KVKey, KVValue) VALUES (?, ?) ON CONFLICT (KVKey) DO UPDATE SET KVValue = EXCLUDED.KVValue;";

    public static int runFunction(PostgresContext ctxt, int key) throws SQLException {
        ResultSet r = ctxt.executeQuery(get, key);
        int value;
        if (r.next()) {
            value = r.getInt(1);
        } else {
            value = 0;
        }
        ctxt.executeUpdate(insert, key, value + 1);
        return value + 1;
    }
}
