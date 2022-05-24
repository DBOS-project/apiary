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
     * Get the current transaction ID.
     * @return transaction ID.
     */
    public long apiaryGetTransactionId() {
        return internalGetTransactionId();
    }

    protected abstract long internalGetTransactionId();

}
