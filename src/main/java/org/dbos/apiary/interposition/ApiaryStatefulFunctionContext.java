package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;

public abstract class ApiaryStatefulFunctionContext extends ApiaryFunctionContext {

    /** Public Interface for functions. **/

    public Object apiaryCallFunction(String name, Object... inputs) {
        // TODO: Logging?
        return internalCallFunction(name, inputs);
    }

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

    /** Abstract and require implementation. **/

    protected abstract Object internalCallFunction(String name, Object... inputs);
    protected abstract void internalExecuteUpdate(Object procedure, Object... input);
    protected abstract Object internalExecuteQuery(Object procedure, Object... input);

}
