package org.dbos.apiary.procedures.cockroachdb;

import org.dbos.apiary.cockroachdb.CockroachDBFunctionInterface;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CockroachDBIncrementFunction extends CockroachDBFunctionInterface {

    private final PreparedStatement getValue;
    private final PreparedStatement updateValue;

    public CockroachDBIncrementFunction(Connection c) throws SQLException {
        this.getValue = c.prepareStatement("SELECT KVVAlue FROM KVTable WHERE KVKey=?;");
        this.updateValue = c.prepareStatement("INSERT INTO KVTable VALUES (?, ?);");
    }

    public String runFunction(String keyString) {
        int key = Integer.parseInt(keyString);
        ResultSet r = (ResultSet) this.apiaryExecuteQuery(getValue, key);
        long nextValue = 0;
        try {
            if (r.next()) {
                nextValue = r.getLong(1) + 1;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.apiaryExecuteUpdate(updateValue, key, nextValue);
        return String.valueOf(nextValue);
    }
}
