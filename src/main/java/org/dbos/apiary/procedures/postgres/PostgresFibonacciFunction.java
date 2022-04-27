package org.dbos.apiary.procedures.postgres;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgresFibonacciFunction extends PostgresFunction {
    private static final String addResult = "INSERT INTO KVTable(KVKey, KVValue) VALUES (?, ?) ON CONFLICT (KVKey) DO NOTHING;";
    private static final String getValue = "SELECT KVValue FROM KVTable WHERE KVKey=?;";

    public static Object runFunction(ApiaryStatefulFunctionContext ctxt, int key) throws SQLException {
        if (key < 0) {
            return "";
        }
        if (key == 0) {
            ctxt.apiaryExecuteUpdate(addResult, key, 0);
            return "0";
        }
        if (key == 1) {
            ctxt.apiaryExecuteUpdate(addResult, key, 1);
            return "1";
        }
        // Check if the number has been calculated before.
        ResultSet r = (ResultSet) ctxt.apiaryExecuteQuery(getValue, key);
        if (r.next()) {
            return String.valueOf(r.getLong(1));
        }
        // Otherwise, call functions.
        ApiaryFuture f1 = ctxt.apiaryQueueFunction("PostgresFibonacciFunction", key - 2);
        ApiaryFuture f2 = ctxt.apiaryQueueFunction("PostgresFibonacciFunction", key - 1);
        ApiaryFuture fsum = ctxt.apiaryQueueFunction("PostgresFibSumFunction", Integer.toString(key), f1, f2);
        return fsum;
    }
}
