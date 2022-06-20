package org.dbos.apiary.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.dbos.apiary.connection.ApiarySecondaryConnection;
import org.dbos.apiary.elasticsearch.ElasticsearchContext;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MongoConnection implements ApiarySecondaryConnection {

    public MongoClient client;
    public MongoDatabase database;

    public MongoConnection(String address, int port) {
        String uri = String.format("mongodb://%s:%d", address, port);
        this.client = MongoClients.create(uri);
        this.database = client.getDatabase("dbos");
    }

    @Override
    public FunctionOutput callFunction(String functionName, WorkerContext workerContext, TransactionContext txc, String service, long execID, long functionID, Object... inputs) throws Exception {
        MongoContext ctxt = new MongoContext(database, workerContext, txc, service, execID, functionID);
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
        return false;
    }

    @Override
    public void garbageCollect(Set<TransactionContext> activeTransactions) {

    }
}
