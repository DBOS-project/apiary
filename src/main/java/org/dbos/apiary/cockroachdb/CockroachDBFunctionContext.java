package org.dbos.apiary.cockroachdb;

import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.ApiaryFunction;
import org.dbos.apiary.function.ApiaryTransactionalContext;
import org.dbos.apiary.function.ProvenanceBuffer;

import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CockroachDBFunctionContext extends ApiaryTransactionalContext {

    public CockroachDBFunctionContext(ProvenanceBuffer provBuff, String service, long execID) {
        super(provBuff, service, execID, 0);
    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) {
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
        return f.apiaryRunFunction(this, inputs);
    }

    @Override
    public FunctionOutput checkPreviousExecution() {
        return null;
    }

    @Override
    public void recordExecution(FunctionOutput output) {

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
            PreparedStatement ps = (PreparedStatement) procedure;
            prepareStatement(ps, input);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void internalExecuteUpdateCaptured(Object procedure, Object... input) {
        internalExecuteUpdate(procedure, input);
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
    protected Object internalExecuteQueryCaptured(Object procedure, Object... input) {
        return internalExecuteQuery(procedure, input);
    }

    @Override
    protected long internalGetTransactionId() {
        return 0;
    }
}
