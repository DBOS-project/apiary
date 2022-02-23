package org.dbos.apiary.procedures.sqlite;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.sqlite.SQLiteFunctionInterface;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.dbos.apiary.utilities.ApiaryConfig.defaultPkey;

public class SQLiteFibonacciFunction extends SQLiteFunctionInterface {

    private final PreparedStatement addResult;
    private final PreparedStatement getValue;

    public SQLiteFibonacciFunction(Connection c) throws SQLException {
        this.addResult = c.prepareStatement("INSERT INTO KVTable(pkey, KVKey, KVValue) VALUES (?, ?, ?);");
        this.getValue = c.prepareStatement("SELECT KVValue FROM KVTable WHERE KVKey=?;");
    }

    public Object runFunction(String strKey) throws SQLException {
        int key = Integer.parseInt(strKey);
        if (key < 0) {
            return "";
        }
        if (key == 0) {
            this.apiaryExecuteUpdate(addResult, defaultPkey, key, 0);
            return "0";
        }
        if (key == 1) {
            this.apiaryExecuteUpdate(addResult, defaultPkey, key, 1);
            return "1";
        }
        // Check if the number has been calculated before.
        ResultSet r = (ResultSet) this.apiaryExecuteQuery(getValue, key);
        if (r.next()) {
            return String.valueOf(r.getLong(1));
        }

        // Otherwise, call functions.
        ApiaryFuture f1 = this.apiaryQueueFunction("FibonacciFunction", defaultPkey, String.valueOf(key - 2));
        ApiaryFuture f2 = this.apiaryQueueFunction("FibonacciFunction", defaultPkey, String.valueOf(key - 1));
        ApiaryFuture fsum = this.apiaryQueueFunction("FibSumFunction", defaultPkey, strKey, f1, f2);
        return fsum;
    }
}
