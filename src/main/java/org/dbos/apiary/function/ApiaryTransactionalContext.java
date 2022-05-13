package org.dbos.apiary.function;

import org.dbos.apiary.utilities.ApiaryConfig;

/**
 * ApiaryTransactionalContext is a context for transactional functions.
 * It provides an interface to query and update the database.
 */
public abstract class ApiaryTransactionalContext extends ApiaryContext {

    public ApiaryTransactionalContext(ProvenanceBuffer provBuff, String service, long execID, long functionID) {
        super(provBuff, service, execID, functionID);
    }
    /** Public Interface for functions. **/

    public abstract FunctionOutput apiaryCallFunction(String name, Object... inputs);

    /**
     * Execute a database update.
     * @param procedure a SQL DML statement (e.g., INSERT, UPDATE, DELETE).
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
     * Execute a database query.
     * @param procedure a SQL query.
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
