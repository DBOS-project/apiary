package org.dbos.apiary.gcs;

import org.dbos.apiary.connection.ApiarySecondaryConnection;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.dbos.apiary.postgres.PostgresConnection;

public class GCSConnection implements ApiarySecondaryConnection {

    public final Storage storage;
    private final PostgresConnection primary;

    public GCSConnection(PostgresConnection primary) {
        this.storage = StorageOptions.getDefaultInstance().getService();
        this.primary = primary;
    }

    @Override
    public FunctionOutput callFunction(String functionName, WorkerContext workerContext,
                                       TransactionContext txc, String service,
                                       long execID, long functionID,
                                       Object... inputs) throws Exception {
        GCSContext ctxt = new GCSContext(storage, workerContext, txc, service, execID, functionID, primary.connection.get());
        FunctionOutput f = null;
        try {
            f = workerContext.getFunction(functionName).apiaryRunFunction(ctxt, inputs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return f;
}

    @Override
    public void rollback(Map<String, List<String>> writtenKeys, TransactionContext txc) {

    }

    @Override
    public boolean validate(Map<String, List<String>> writtenKeys, TransactionContext txc) {
        return true;
    }

    @Override
    public void garbageCollect(Set<TransactionContext> activeTransactions) {

    }
}
