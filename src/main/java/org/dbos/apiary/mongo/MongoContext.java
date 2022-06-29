package org.dbos.apiary.mongo;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MongoContext extends ApiaryContext {
    private static final Logger logger = LoggerFactory.getLogger(MongoConnection.class);
    
    public static final String apiaryID = "__apiaryID__";
    public static final String beginVersion = "__beginVersion__";
    public static final String endVersion = "__endVersion__";

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
        if (!ApiaryConfig.XDBTransactions) {
            database.getCollection(collectionName).insertOne(document);
            return;
        }
        document.append(apiaryID, id);
        document.append(beginVersion, txc.txID);
        document.append(endVersion, Long.MAX_VALUE);
        writtenKeys.putIfAbsent(collectionName, new ArrayList<>());
        writtenKeys.get(collectionName).add(id);
        database.getCollection(collectionName).insertOne(document);
    }

    public void insertMany(String collectionName, List<Document> documents, List<String> ids) {
        if (!ApiaryConfig.XDBTransactions) {
            MongoCollection<Document> collection = database.getCollection(collectionName);
            collection.bulkWrite(documents.stream().map(InsertOneModel::new).collect(Collectors.toList()), new BulkWriteOptions().ordered(false));
            return;
        }
        for (int i = 0; i < documents.size(); i++) {
            Document d = documents.get(i);
            d.append(apiaryID, ids.get(i));
            d.append(beginVersion, txc.txID);
            d.append(endVersion, Long.MAX_VALUE);
            writtenKeys.putIfAbsent(collectionName, new ArrayList<>());
            writtenKeys.get(collectionName).add(ids.get(i));
        }
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.bulkWrite(documents.stream().map(InsertOneModel::new).collect(Collectors.toList()), new BulkWriteOptions().ordered(false));
    }

    public FindIterable<Document> find(String collectionName, Bson filter) {
        if (!ApiaryConfig.XDBTransactions) {
            return database.getCollection(collectionName).find(filter);
        }
        List<Bson> beginVersionFilter = new ArrayList<>();
        beginVersionFilter.add(Filters.lt(beginVersion, txc.xmax));
        for (long txID: txc.activeTransactions) {
            beginVersionFilter.add(Filters.ne(beginVersion, txID));
        }
        List<Bson> endVersionFilter = new ArrayList<>();
        endVersionFilter.add(Filters.gte(endVersion, txc.xmax));
        for (long txID: txc.activeTransactions) {
            endVersionFilter.add(Filters.eq(endVersion, txID));
        }
        Bson query = Filters.and(
                Filters.and(beginVersionFilter),
                Filters.or(endVersionFilter),
                filter
        );
        MongoCollection<Document> collection = database.getCollection(collectionName);
        return collection.find(query);
    }

    public AggregateIterable<Document> aggregate(String collectionName, List<Bson> aggregations) {
        if (!ApiaryConfig.XDBTransactions) {
            return database.getCollection(collectionName).aggregate(aggregations);
        }
        List<Bson> beginVersionFilter = new ArrayList<>();
        beginVersionFilter.add(Filters.lt(beginVersion, txc.xmax));
        for (long txID: txc.activeTransactions) {
            beginVersionFilter.add(Filters.ne(beginVersion, txID));
        }
        List<Bson> endVersionFilter = new ArrayList<>();
        endVersionFilter.add(Filters.gte(endVersion, txc.xmax));
        for (long txID: txc.activeTransactions) {
            endVersionFilter.add(Filters.eq(endVersion, txID));
        }
        Bson filter = Aggregates.match(
                Filters.and(
                        Filters.and(beginVersionFilter),
                        Filters.or(endVersionFilter)
                )
        );
        MongoCollection<Document> collection = database.getCollection(collectionName);
        aggregations = new ArrayList<>(aggregations);
        aggregations.add(0, filter);
        AggregateIterable<Document> r = collection.aggregate(aggregations);
        return r;
    }
}
