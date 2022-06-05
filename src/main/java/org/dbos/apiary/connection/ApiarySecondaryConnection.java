package org.dbos.apiary.connection;

import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;

/**
 * A connection to a secondary database.
 */
public interface ApiarySecondaryConnection {
    /**
     * For internal use only.
     * @param functionName
     * @param workerContext
     * @param transactionContext
     * @param service
     * @param execID
     * @param functionID
     * @param inputs
     * @return
     * @throws Exception
     */
    FunctionOutput callFunction(String functionName, WorkerContext workerContext, TransactionContext transactionContext, String service, long execID, long functionID, Object... inputs) throws Exception;

    // For partition mapping information.


}
