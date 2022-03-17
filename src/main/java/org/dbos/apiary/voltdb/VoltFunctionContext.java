package org.dbos.apiary.voltdb;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

public class VoltFunctionContext extends ApiaryStatefulFunctionContext {

    private final VoltApiaryProcedure p;

    public VoltFunctionContext(VoltApiaryProcedure p) {
        this.p = p;
    }

    @Override
    protected void internalExecuteUpdate(Object procedure, Object... input) {
        p.voltQueueSQL((SQLStmt) procedure, input);
        p.voltExecuteSQL();
    }

    @Override
    protected VoltTable[] internalExecuteQuery(Object procedure, Object... input) {
        p.voltQueueSQL((SQLStmt) procedure, input);
        return p.voltExecuteSQL();
    }

}
