package org.dbos.apiary.sqlite;

import org.dbos.apiary.interposition.ApiaryFunctionInterface;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SQLiteFunctionInterface extends ApiaryFunctionInterface {
    @Override
    protected void internalExecuteUpdate(Object procedure, Object... input) {
        try {
            PreparedStatement ps = (PreparedStatement) procedure;
            for (int i = 0; i < input.length; i++) {
                Object o = input[i];
                if (o instanceof Integer) {
                    ps.setInt(i + 1, (Integer) o);
                } else if (o instanceof String) {
                    ps.setString(i + 1, (String) o);
                } else {
                    assert (false); // TODO: More types.
                }
            }
            ps.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Object internalExecuteQuery(Object procedure, Object... input) {
        try {
            PreparedStatement ps = (PreparedStatement) procedure;
            ps.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected Object internalRunFunction(Object... input) {
        return null;
    }
}
