package org.dbos.apiary.connection;

import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;

import java.util.Map;
import java.util.Set;

/**
 * A connection to a primary database.
 */
public interface ApiaryConnection {
    /**
     * For internal use only.
     * @param functionName
     * @param workerContext
     * @param service
     * @param execID
     * @param functionID
     * @param inputs
     * @return
     * @throws Exception
     */
    FunctionOutput callFunction(String functionName, WorkerContext workerContext, String service, long execID, long functionID, Object... inputs) throws Exception;

    public Set<TransactionContext> getActiveTransactions();

    // For partition mapping information.

    /**
     * For internal use only.
     */
    void updatePartitionInfo();

    /**
     * For internal use only.
     * @return
     */
    int getNumPartitions();

    /**
     * For internal use only.
     * @param input
     * @return
     */
    String getHostname(Object... input);  // Return the hostname to which to send an input.

    /**
     * For internal use only.
     * @return
     */
    Map<Integer, String> getPartitionHostMap();

}
