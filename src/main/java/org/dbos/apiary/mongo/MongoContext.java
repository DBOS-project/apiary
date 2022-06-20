package org.dbos.apiary.mongo;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoContext extends ApiaryContext {
    private static final String apiaryID = "__apiaryID__";
    private static final String beginVersion = "__beginVersion__";
    private static final String endVersion = "__endVersion__";


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

    public void insertOne(String collectionName, Document document, String id) {
        document.append(apiaryID, id);
        document.append(beginVersion, txc.txID);
        document.append(endVersion, Long.MAX_VALUE);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(document);
    }

    public AggregateIterable<Document> aggregate(String collectionName, List<Bson> aggregations) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        return collection.aggregate(aggregations);
    }
}
