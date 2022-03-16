package org.dbos.apiary.sqlite;

import org.dbos.apiary.interposition.ApiaryStatefulFunction;
import org.dbos.apiary.utilities.Utilities;

import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SQLiteFunctionInterface extends ApiaryStatefulFunction {

    private void prepareStatement(PreparedStatement ps, Object[] input) throws SQLException {
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
    }

    @Override
    public Object internalCallFunction(String name, Object... inputs) {
        return null;
    }

    @Override
    protected void internalExecuteUpdate(Object procedure, Object... input) {
        try {
            PreparedStatement ps = (PreparedStatement) procedure;
            prepareStatement(ps, input);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Object internalExecuteQuery(Object procedure, Object... input) {
        try {
            PreparedStatement ps = (PreparedStatement) procedure;
            prepareStatement(ps, input);
            return ps.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected Object internalRunFunction(Object... input) {
        // Use reflection to find internal runFunction.
        Method functionMethod = Utilities.getFunctionMethod(this, "runFunction");
        assert functionMethod != null;
        Object output;
        try {
            output = functionMethod.invoke(this, input);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return output;
    }
}
