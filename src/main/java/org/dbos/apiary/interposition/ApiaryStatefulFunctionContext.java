package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.utilities.ApiaryConfig;

public abstract class ApiaryStatefulFunctionContext extends ApiaryFunctionContext {

    public ApiaryStatefulFunctionContext(ProvenanceBuffer provBuff, String service, long execID, long functionID) {
        super(provBuff, service, execID, functionID);
    }
    /** Public Interface for functions. **/

    public abstract FunctionOutput apiaryCallFunction(String name, Object... inputs);

    // Execute an update in the database.
    public void apiaryExecuteUpdate(Object procedure, Object... input) {
        if (ApiaryConfig.captureUpdates && (this.provBuff != null)) {
            internalExecuteUpdateCaptured(procedure, input);
        } else {
            internalExecuteUpdate(procedure, input);
        }
    }

    // Execute a database query.
    public Object apiaryExecuteQuery(Object procedure, Object... input) {
        if (ApiaryConfig.captureReads && (this.provBuff != null)) {
            return internalExecuteQueryCaptured(procedure, input);
        } else {
            return internalExecuteQuery(procedure, input);
        }
    }

    // Get the current transaction ID.
    public long apiaryGetTransactionId() {
        return internalGetTransactionId();
    }

    /** Abstract and require implementation. **/
    protected abstract void internalExecuteUpdate(Object procedure, Object... input);
    protected abstract void internalExecuteUpdateCaptured(Object procedure, Object... input);
    protected abstract Object internalExecuteQuery(Object procedure, Object... input);
    protected abstract Object internalExecuteQueryCaptured(Object procedure, Object... input);

    protected abstract long internalGetTransactionId();

}
