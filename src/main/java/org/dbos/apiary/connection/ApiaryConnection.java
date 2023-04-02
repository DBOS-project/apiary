package org.dbos.apiary.connection;

import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.postgres.PostgresContext;

import java.sql.Connection;
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
     * @param role
     * @param execID
     * @param functionID
     * @param inputs
     * @return
     * @throws Exception
     */
    FunctionOutput callFunction(String functionName, WorkerContext workerContext, String role, long execID, long functionID,
                                int replayMode, Object... inputs) throws Exception;

    default FunctionOutput replayFunction(ApiaryContext pgCtxt, String functionName, Set<String> replayWrittenTables,
                                  Object... inputs) {
        return null;
    }

    Set<TransactionContext> getActiveTransactions();

    TransactionContext getLatestTransactionContext();

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

    default Connection createNewConnection() {
        return null;
    }

}
