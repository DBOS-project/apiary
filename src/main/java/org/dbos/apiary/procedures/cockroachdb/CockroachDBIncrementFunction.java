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

    public String runFunction(String keyString1, String keyString2) {
        int key1 = Integer.parseInt(keyString1);
        int key2 = Integer.parseInt(keyString2);
        ResultSet r1 = (ResultSet) context.apiaryExecuteQuery(getValue, key1);
        ResultSet r2 = (ResultSet) context.apiaryExecuteQuery(getValue, key2);
        int nextValue = 0;
        try {
            if (r1.next()) {
                nextValue += r1.getInt(1);
            }
            if (r2.next()) {
                nextValue += r2.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        context.apiaryExecuteUpdate(updateValue, key1, nextValue);
        return String.valueOf(nextValue);
    }
}
