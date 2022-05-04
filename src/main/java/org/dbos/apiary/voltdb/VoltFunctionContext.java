package org.dbos.apiary.voltdb;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.interposition.ApiaryFunction;
import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.interposition.ProvenanceBuffer;
import org.voltdb.DeprecatedProcedureAPIAccess;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class VoltFunctionContext extends ApiaryStatefulFunctionContext {

    private final VoltApiaryProcedure p;

    public VoltFunctionContext(VoltApiaryProcedure p, ProvenanceBuffer provBuff, String service, long execID) {
        // TODO: add actual provenance buffer, service name, and execution ID.
        super(provBuff, service, execID);
        this.p = p;
    }

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
        return f.apiaryRunFunction(ctxt, inputs);
    }

    @Override
    protected void internalExecuteUpdate(Object procedure, Object... input) {
        p.voltQueueSQL((SQLStmt) procedure, input);
        p.voltExecuteSQL();
    }

    @Override
    protected void internalExecuteUpdateCaptured(Object procedure, Object... input) {
        // TODO: implement provenance capture.
        internalExecuteUpdate(procedure, input);
    }

    @Override
    protected VoltTable[] internalExecuteQuery(Object procedure, Object... input) {
        p.voltQueueSQL((SQLStmt) procedure, input);
        return p.voltExecuteSQL();
    }

    @Override
    protected Object internalExecuteQueryCaptured(Object procedure, Object... input) {
        // TODO: implement provenance capture.
        return internalExecuteQuery(procedure, input);
    }

    @Override
    protected long internalGetTransactionId() {
        return DeprecatedProcedureAPIAccess.getVoltPrivateRealTransactionId(this.p);
    }
}
