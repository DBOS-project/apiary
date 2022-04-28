package org.dbos.apiary.postgres;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.interposition.ApiaryFunction;
import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;

public class PostgresFunctionContext extends ApiaryStatefulFunctionContext {
    // This connection ties to all prepared statements in one transaction.
    private final Connection conn;

    public PostgresFunctionContext(Connection c) { this.conn= c; }

    @Override
    public FunctionOutput apiaryCallFunction(ApiaryFunctionContext ctxt, String name, Object... inputs) {
        // TODO: Logging?
        Object clazz;
        try {
            clazz = Class.forName(name).getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
            return null;
        }
        assert(clazz instanceof ApiaryFunction);
        ApiaryFunction f = (ApiaryFunction) clazz;
        try {
            Savepoint s = conn.setSavepoint();
            try {
                FunctionOutput o = f.apiaryRunFunction(ctxt, inputs);
                conn.releaseSavepoint(s);
                return o;
            } catch (Exception e) {
                e.printStackTrace();
                conn.rollback(s);
                conn.releaseSavepoint(s);
                return null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

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
    protected void internalExecuteUpdate(Object procedure, Object... input) {
        try {
            // First, prepare statement. Then, execute.
            PreparedStatement pstmt = conn.prepareStatement((String) procedure);
            prepareStatement(pstmt, input);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Object internalExecuteQuery(Object procedure, Object... input) {
        try {
            // First, prepare statement. Then, execute.
            PreparedStatement pstmt = conn.prepareStatement((String) procedure);
            prepareStatement(pstmt, input);
            return pstmt.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
}
