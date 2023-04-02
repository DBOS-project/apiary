package org.dbos.apiary.connection;

import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A connection to a secondary database.
 */
public interface ApiarySecondaryConnection {

    FunctionOutput callFunction(String functionName, Map<String, List<String>> writtenKeys, WorkerContext workerContext, TransactionContext transactionContext, String role, long execID, long functionID, Object... inputs) throws Exception;

    // Map is from a table/index to a list of written keys for that index.
    void rollback(Map<String, List<String>> writtenKeys, TransactionContext txc);

    boolean validate(Map<String, List<String>> writtenKeys, TransactionContext txc);

    void commit(Map<String, List<String>> writtenKeys, TransactionContext txc);

    void garbageCollect(Set<TransactionContext> activeTransactions);
}
