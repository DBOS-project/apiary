package org.dbos.apiary.procedures.cockroachdb;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.cockroachdb.CockroachDBFunctionInterface;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CockroachDBFibonacciFunction extends CockroachDBFunctionInterface {

    private final PreparedStatement addResult;
    private final PreparedStatement getValue;

    public CockroachDBFibonacciFunction(Connection c) throws SQLException {
        this.addResult = c.prepareStatement("UPSERT INTO KVTable(KVKey, KVValue) VALUES (?, ?);");
        this.getValue = c.prepareStatement("SELECT KVValue FROM KVTable WHERE KVKey=?;");
    }

    public Object runFunction(String strKey) throws SQLException {
        int key = Integer.parseInt(strKey);
        if (key < 0) {
            return "";
        }
        if (key == 0) {
            this.apiaryExecuteUpdate(addResult, key, 0);
            return "0";
        }
        if (key == 1) {
            this.apiaryExecuteUpdate(addResult, key, 1);
            return "1";
        }
        // Check if the number has been calculated before.
        ResultSet r = (ResultSet) this.apiaryExecuteQuery(getValue, key);
        if (r.next()) {
            return String.valueOf(r.getLong(1));
        }

        // Otherwise, call functions.
        ApiaryFuture f1 = this.apiaryQueueFunction("FibonacciFunction", String.valueOf(key - 2));
        ApiaryFuture f2 = this.apiaryQueueFunction("FibonacciFunction", String.valueOf(key - 1));
        ApiaryFuture fsum = this.apiaryQueueFunction("FibSumFunction", strKey, f1, f2);
        return fsum;
    }
}
