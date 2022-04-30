package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.utilities.ApiaryConfig;

import java.lang.reflect.InvocationTargetException;

public abstract class ApiaryStatefulFunctionContext extends ApiaryFunctionContext {

    public ApiaryStatefulFunctionContext(ProvenanceBuffer provBuff, String service, long execID) {
        super(provBuff, service, execID);
    }
    /** Public Interface for functions. **/

    public abstract FunctionOutput apiaryCallFunction(ApiaryFunctionContext ctxt, String name, Object... inputs);

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
        return internalExecuteQuery(procedure, input);
    }

    // TODO: a more elegant way to handle read capture?
    public Object apiaryExecuteQueryCaptured(Object procedure, int[] primaryKeyCols, Object... input) {
        if (ApiaryConfig.captureReads && (this.provBuff != null)) {
            return internalExecuteQueryCaptured(procedure, primaryKeyCols, input);
        }
        // Do not capture if configured to not capture reads.
        return internalExecuteQuery(procedure, input);
    }

    // Get the current transaction ID.
    public long apiaryGetTransactionId() {
        return internalGetTransactionId();
    }

    /** Abstract and require implementation. **/
    protected abstract void internalExecuteUpdate(Object procedure, Object... input);
    protected abstract void internalExecuteUpdateCaptured(Object procedure, Object... input);
    protected abstract Object internalExecuteQuery(Object procedure, Object... input);
    protected abstract Object internalExecuteQueryCaptured(Object procedure, int[] primaryKeyCols, Object... input);

    protected abstract long internalGetTransactionId();

}
