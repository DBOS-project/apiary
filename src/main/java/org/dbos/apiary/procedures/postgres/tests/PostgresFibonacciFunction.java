package org.dbos.apiary.procedures.postgres.tests;

import org.dbos.apiary.function.ApiaryFuture;
import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgresFibonacciFunction extends PostgresFunction {
    private static final String addResult = "INSERT INTO KVTable(KVKey, KVValue) VALUES (?, ?) ON CONFLICT (KVKey) DO NOTHING;";
    private static final String getValue = "SELECT KVValue FROM KVTable WHERE KVKey=?;";

    public static Object runFunction(PostgresContext ctxt, int key) throws SQLException {
        if (key < 0) {
            return -1;
        }
        if (key == 0) {
            ctxt.executeUpdate(addResult, key, 0);
            return 0;
        }
        if (key == 1) {
            ctxt.executeUpdate(addResult, key, 1);
            return 1;
        }
        // Check if the number has been calculated before.
        ResultSet r = ctxt.executeQuery(getValue, key);
        if (r.next()) {
            return r.getInt(1);
        }
        // Otherwise, call functions.
        ApiaryFuture f1 = ctxt.apiaryQueueFunction("PostgresFibonacciFunction", key - 2);
        ApiaryFuture f2 = ctxt.apiaryQueueFunction("PostgresFibonacciFunction", key - 1);
        return ctxt.apiaryQueueFunction("PostgresFibSumFunction", key, f1, f2);
    }
}
