package org.dbos.apiary.connection;

import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;

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
     * @param service
     * @param execID
     * @param functionID
     * @param inputs
     * @return
     * @throws Exception
     */
    FunctionOutput callFunction(String functionName, WorkerContext workerContext, String service, long execID, long functionID,
                                int replayMode, Object... inputs) throws Exception;

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

    /**
     * for internal use only. Similar to callFunction, but is only used for replay, because the worker can explicitly specify a connection to the database.
     * @param conn
     * @param functionName
     * @param workerContext
     * @param service
     * @param execID
     * @param functionID
     * @param replayMode
     * @param inputs
     * @return
     */
    default FunctionOutput replayFunction(Connection conn, String functionName, WorkerContext workerContext,
                                          String service, long execID, long functionID, int replayMode,
                                          Object... inputs) {
        return null;
    }

}
