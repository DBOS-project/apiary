package org.dbos.apiary.function;

/**
 * ApiaryTransactionalContext is the base class for transactional function contexts.
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
