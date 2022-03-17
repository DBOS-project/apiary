package org.dbos.apiary.procedures.cockroachdb;

import org.dbos.apiary.cockroachdb.CockroachDBFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CockroachDBIncrementFunction extends CockroachDBFunction {

    private final PreparedStatement getValue;
    private final PreparedStatement updateValue;

    public CockroachDBIncrementFunction(Connection c) throws SQLException {
        this.getValue = c.prepareStatement("SELECT KVVAlue FROM KVTable WHERE KVKey=?;");
        this.updateValue = c.prepareStatement("UPSERT INTO KVTable(KVKey, KVValue) VALUES (?, ?);");
    }

    public String runFunction(String keyString) {
        int key = Integer.parseInt(keyString);
        ResultSet r = (ResultSet) context.apiaryExecuteQuery(getValue, key);
        int nextValue = 0;
        try {
            if (r.next()) {
                nextValue = r.getInt(1) + 1;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        context.apiaryExecuteUpdate(updateValue, key, nextValue);
        return String.valueOf(nextValue);
    }
}
