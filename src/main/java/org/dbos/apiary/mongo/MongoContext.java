package org.dbos.apiary.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoContext extends ApiaryContext {
    private final MongoDatabase database;
    private final TransactionContext txc;

    Map<String, List<String>> writtenKeys = new HashMap<>();

    public MongoContext(MongoDatabase database, WorkerContext workerContext, TransactionContext txc, String service, long execID, long functionID) {
        super(workerContext, service, execID, functionID);
        this.database = database;
        this.txc = txc;
    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) throws Exception {
        // TODO: Implement.
        return null;
    }

    public void executeWrite(String collectionName, Document document) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(document);
    }

    public Document find(String collectionName, Document search) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        return collection.find(search).first();
    }
}
