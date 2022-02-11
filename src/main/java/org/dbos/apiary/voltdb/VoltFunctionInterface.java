package org.dbos.apiary.voltdb;

import org.dbos.apiary.interposition.ApiaryFunctionInterface;
import org.voltdb.SQLStmt;

public class VoltFunctionInterface extends ApiaryFunctionInterface {

    private final VoltApiaryProcedure p;

    public VoltFunctionInterface(VoltApiaryProcedure p) {
        this.p = p;
    }

    @Override
    protected void internalQueueSQL(Object procedure, Object... input) {
        p.voltQueueSQL((SQLStmt) procedure, input);
    }

    @Override
    protected void internalExecuteSQL() {
        p.voltExecuteSQL();
    }

    @Override
    protected Object internalRunFunction(Object... input) {
        // TODO: Weird reflection stuff.
        return null;
    }
}
