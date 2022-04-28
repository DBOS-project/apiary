package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;

import java.lang.reflect.InvocationTargetException;

public abstract class ApiaryStatefulFunctionContext extends ApiaryFunctionContext {

    public ApiaryStatefulFunctionContext(ProvenanceBuffer provBuff, String service, long execID) {
        super(provBuff, service, execID);
    }
    /** Public Interface for functions. **/

    public abstract FunctionOutput apiaryCallFunction(ApiaryFunctionContext ctxt, String name, Object... inputs);

    // Execute an update in the database.
    public void apiaryExecuteUpdate(Object procedure, Object... input) {
        // TODO: Provenance capture.
        internalExecuteUpdate(procedure, input);
    }

    // Execute a database query.
    public Object apiaryExecuteQuery(Object procedure, Object... input) {
        // TODO: Provenance capture.
        return internalExecuteQuery(procedure, input);
    }

    // Get the current transaction ID.
    public long apiaryGetTransactionId() {
        return internalGetTransactionId();
    }

    /** Abstract and require implementation. **/
    protected abstract void internalExecuteUpdate(Object procedure, Object... input);
    protected abstract Object internalExecuteQuery(Object procedure, Object... input);

    protected abstract long internalGetTransactionId();

}
