package org.dbos.apiary.mongo;

import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class MongoContext extends ApiaryContext {
    private static final Logger logger = LoggerFactory.getLogger(MongoConnection.class);
    
    public static final String apiaryID = "__apiaryID__";
    public static final String beginVersion = "__beginVersion__";
    public static final String endVersion = "__endVersion__";
    public static final String committed = "__committed__";

    private final MongoClient client;
    private final MongoDatabase database;
    private final TransactionContext txc;
    private final Map<String, Map<String, AtomicBoolean>> lockManager;

    private boolean mongoUpdated = false;

    final Map<String, List<String>> writtenKeys;

    public MongoContext(MongoClient client, Map<String, List<String>> writtenKeys, MongoDatabase database, Map<String, Map<String, AtomicBoolean>> lockManager, WorkerContext workerContext, TransactionContext txc, String role, long execID, long functionID) {
        super(workerContext, role, execID, functionID, ApiaryConfig.ReplayMode.NOT_REPLAY.getValue());
        this.database = database;
        this.client = client;
        this.txc = txc;
        this.lockManager = lockManager;
        this.writtenKeys = writtenKeys;
    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) throws Exception {
        // TODO: Implement.
        return null;
    }

    public void insertOne(String collectionName, Document document, String id) throws PSQLException {
        if (!ApiaryConfig.XDBTransactions) {
            document.append(apiaryID, id);
            database.getCollection(collectionName).insertOne(document);
            return;
        }
        lockManager.putIfAbsent(collectionName, new ConcurrentHashMap<>());
        lockManager.get(collectionName).putIfAbsent(id, new AtomicBoolean(false));
        boolean available = lockManager.get(collectionName).get(id).compareAndSet(false, true);
        if (!available) {
            throw new PSQLException("tuple locked", PSQLState.SERIALIZATION_FAILURE);
        }
        document.append(apiaryID, id);
        document.append(beginVersion, txc.txID);
        document.append(endVersion, Long.MAX_VALUE);
        MongoCollection<Document> c = database.getCollection(collectionName);
        boolean exists = c.find(Filters.eq(MongoContext.apiaryID, id)).first() != null;
        if (!exists) {
            c.insertOne(document);
            writtenKeys.putIfAbsent(collectionName, new ArrayList<>());
            writtenKeys.get(collectionName).add(id);
            mongoUpdated = true;
        } else {
            logger.info("Insert failed, exists: {}", id);
        }
    }

    public void replaceOne(String collectionName, Document document, String id) throws PSQLException {
        if (!ApiaryConfig.XDBTransactions) {
            document.append(apiaryID, id);
            database.getCollection(collectionName).replaceOne(Filters.eq(MongoContext.apiaryID, id), document);
            return;
        }
        lockManager.putIfAbsent(collectionName, new ConcurrentHashMap<>());
        lockManager.get(collectionName).putIfAbsent(id, new AtomicBoolean(false));
        boolean available = lockManager.get(collectionName).get(id).compareAndSet(false, true);
        if (!available) {
            throw new PSQLException("tuple locked", PSQLState.SERIALIZATION_FAILURE);
        }
        document.append(apiaryID, id);
        document.append(beginVersion, txc.txID);
        document.append(endVersion, Long.MAX_VALUE);
        MongoCollection<Document> c = database.getCollection(collectionName);
        c.insertOne(document);
        c.updateOne(Filters.and(
                        Filters.eq(MongoContext.apiaryID, id),
                        Filters.ne(MongoContext.beginVersion, txc.txID),
                        Filters.eq(MongoContext.endVersion, Long.MAX_VALUE)
                ),
                Updates.set(MongoContext.endVersion, txc.txID)
        );
        writtenKeys.putIfAbsent(collectionName, new ArrayList<>());
        writtenKeys.get(collectionName).add(id);
        mongoUpdated = true;
    }

    public void insertMany(String collectionName, List<Document> documents, List<String> ids) {
        if (!ApiaryConfig.XDBTransactions) {
            for (int i = 0; i < documents.size(); i++) {
                documents.get(i).append(apiaryID, ids.get(i));
            }
            MongoCollection<Document> collection = database.getCollection(collectionName);
            collection.bulkWrite(documents.stream().map(InsertOneModel::new).collect(Collectors.toList()), new BulkWriteOptions().ordered(false));
            return;
        }
        for (int i = 0; i < documents.size(); i++) {
            Document d = documents.get(i);
            d.append(apiaryID, ids.get(i));
            d.append(beginVersion, txc.txID);
            d.append(endVersion, Long.MAX_VALUE);
        }
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.bulkWrite(documents.stream().map(InsertOneModel::new).collect(Collectors.toList()), new BulkWriteOptions().ordered(false));
        mongoUpdated = true;
    }

    public FindIterable<Document> find(String collectionName, Bson filter) {
        if (!ApiaryConfig.XDBTransactions) {
            return database.getCollection(collectionName).find(filter);
        }
        Bson query;
        List<Bson> beginVersionFilter = new ArrayList<>();
        beginVersionFilter.add(Filters.lt(beginVersion, txc.xmax));
        for (long txID : txc.activeTransactions) {
            beginVersionFilter.add(Filters.ne(beginVersion, txID));
        }
        List<Bson> endVersionFilter = new ArrayList<>();
        endVersionFilter.add(Filters.gte(endVersion, txc.xmax));
        for (long txID : txc.activeTransactions) {
            endVersionFilter.add(Filters.eq(endVersion, txID));
        }
        if (mongoUpdated) {
            query = Filters.and(
                    Filters.or(
                            Filters.and(beginVersionFilter),
                            Filters.eq(beginVersion, txc.txID)
                    ),
                    Filters.or(endVersionFilter),
                    Filters.ne(endVersion, txc.txID),
                    filter
            );
        } else {
            query = Filters.and(
                    Filters.and(beginVersionFilter),
                    Filters.or(endVersionFilter),
                    filter
            );
        }
        MongoCollection<Document> collection = database.getCollection(collectionName);
        return collection.find(query);
    }

    public AggregateIterable<Document> aggregate(String collectionName, List<Bson> aggregations) {
        if (!ApiaryConfig.XDBTransactions) {
            return database.getCollection(collectionName).aggregate(aggregations);
        }
        Bson filter;
        List<Bson> beginVersionFilter = new ArrayList<>();
        beginVersionFilter.add(Filters.lt(beginVersion, txc.xmax));
        for (long txID : txc.activeTransactions) {
            beginVersionFilter.add(Filters.ne(beginVersion, txID));
        }
        List<Bson> endVersionFilter = new ArrayList<>();
        endVersionFilter.add(Filters.gte(endVersion, txc.xmax));
        for (long txID : txc.activeTransactions) {
            endVersionFilter.add(Filters.eq(endVersion, txID));
        }
        if (mongoUpdated) {
            filter = Aggregates.match(
                    Filters.and(
                            Filters.or(
                                    Filters.and(beginVersionFilter),
                                    Filters.eq(beginVersion, txc.txID)
                            ),
                            Filters.or(endVersionFilter),
                            Filters.ne(endVersion, txc.txID)
                    )
            );
        } else {
            filter = Aggregates.match(
                    Filters.and(
                            Filters.and(beginVersionFilter),
                            Filters.or(endVersionFilter)
                    )
            );
        }
        MongoCollection<Document> collection = database.getCollection(collectionName);
        List<Bson> filterAggregations = new ArrayList<>(aggregations);
        filterAggregations.add(0, filter);
        return collection.aggregate(filterAggregations);
    }
}
