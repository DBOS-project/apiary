package org.dbos.apiary.function;

import org.dbos.apiary.utilities.ApiaryConfig;

/**
 * ApiaryTransactionalContext is for functions that interact with databases.
 * It provides additional APIs to execute database updates and queries.
 */
public abstract class ApiaryTransactionalContext extends ApiaryContext {

    public ApiaryTransactionalContext(ProvenanceBuffer provBuff, String service, long execID, long functionID) {
        super(provBuff, service, execID, functionID);
    }
    /** Public Interface for functions. **/

    public abstract FunctionOutput apiaryCallFunction(String name, Object... inputs);

    /**
     * Execute an update query in the database backend.
     * @param procedure SQL statement.
     * @param input     input parameters for the SQL statement.
     */
    public void apiaryExecuteUpdate(Object procedure, Object... input) {
        if (ApiaryConfig.captureUpdates && (this.provBuff != null)) {
            internalExecuteUpdateCaptured(procedure, input);
        } else {
            internalExecuteUpdate(procedure, input);
        }
    }

    /**
     * Execute a read-only query in the database backend.
     * @param procedure SQL statement (read-only).
     * @param input     input parameters for the SQL statement.
     */
    public Object apiaryExecuteQuery(Object procedure, Object... input) {
        if (ApiaryConfig.captureReads && (this.provBuff != null)) {
            return internalExecuteQueryCaptured(procedure, input);
        } else {
            return internalExecuteQuery(procedure, input);
        }
    }

    /**
     * Get the current transaction ID.
     * @return transaction ID.
     */
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
