package org.dbos.apiary.mongo;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.dbos.apiary.connection.ApiarySecondaryConnection;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MongoConnection implements ApiarySecondaryConnection {
    private static final Logger logger = LoggerFactory.getLogger(MongoConnection.class);

    public MongoClient client;
    public MongoDatabase database;

    private final Map<String, Map<String, Set<Long>>> committedWrites = new ConcurrentHashMap<>();
    private final Lock validationLock = new ReentrantLock();
    Map<String, Map<String, AtomicBoolean>> lockManager = new ConcurrentHashMap<>();

    public MongoConnection(String address, int port) {
        String uri = String.format("mongodb://%s:%d", address, port);
        this.client = MongoClients.create(uri);
        this.database = client.getDatabase("dbos");
    }

    @Override
    public FunctionOutput callFunction(String functionName, WorkerContext workerContext, TransactionContext txc, String service, long execID, long functionID, Object... inputs) throws Exception {
        MongoContext ctxt = new MongoContext(client, database, lockManager, workerContext, txc, service, execID, functionID);
        return workerContext.getFunction(functionName).apiaryRunFunction(ctxt, inputs);
    }

    @Override
    public void rollback(Map<String, List<String>> writtenKeys, TransactionContext txc) {
        for (String collection: writtenKeys.keySet()) {
            MongoCollection<Document> c = database.getCollection(collection);
            c.deleteMany(Filters.eq(MongoContext.beginVersion, txc.txID));
            c.updateMany(Filters.and(
                            Filters.eq(MongoContext.endVersion, txc.txID)
                    ),
                    Updates.set(MongoContext.endVersion, Long.MAX_VALUE)
            );
            for (String key: writtenKeys.get(collection)) {
                lockManager.get(collection).get(key).set(false);
            }
        }
    }

    @Override
    public boolean validate(Map<String, List<String>> writtenKeys, TransactionContext txc) {
        for (String collection: writtenKeys.keySet()) {
            for (String key: writtenKeys.get(collection)) {
                lockManager.get(collection).get(key).set(false);
            }
        }
        return true;
    }

    @Override
    public void rcCommit(Map<String, List<String>> writtenKeys, TransactionContext txc) {
        ClientSession session = client.startSession();
        TransactionBody<Boolean> txnBody = () -> {
            for (String collectionName: writtenKeys.keySet()) {
                if (writtenKeys.get(collectionName).size() >= 10000) {
                    continue; // Speed up bulk-loading in benchmarks.
                }
                MongoCollection<Document> c = database.getCollection(collectionName);
                for (String key: writtenKeys.get(collectionName)) {
                    c.updateOne(session, Filters.and(
                                    Filters.eq(MongoContext.apiaryID, key),
                                    Filters.eq(MongoContext.beginVersion, txc.txID)
                            ),
                            Updates.set(MongoContext.committed, true)
                    );
                    c.updateMany(session, Filters.and(
                                    Filters.eq(MongoContext.apiaryID, key),
                                    Filters.ne(MongoContext.beginVersion, txc.txID)
                            ),
                            Updates.set(MongoContext.committed, false)
                    );
                }
            }
            return Boolean.TRUE;
        };
        session.withTransaction(txnBody, TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).readConcern(ReadConcern.SNAPSHOT).build());
        session.close();
    }

    @Override
    public void garbageCollect(Set<TransactionContext> activeTransactions) {
        long globalxmin = activeTransactions.stream().mapToLong(i -> i.xmin).min().getAsLong();
        // No need to keep track of writes that are visible to all active or future transactions.
        committedWrites.values().forEach(i -> i.values().forEach(w -> w.removeIf(txID -> txID < globalxmin)));
        // Delete old versions that are no longer visible to any active or future transaction.
        if (ApiaryConfig.isolationLevel == ApiaryConfig.REPEATABLE_READ) {
            for (String collectionName : committedWrites.keySet()) {
                MongoCollection<Document> c = database.getCollection(collectionName);
                c.deleteMany(
                        Filters.lt(MongoContext.endVersion, globalxmin)
                );
            }
        } else {
            assert (ApiaryConfig.isolationLevel == ApiaryConfig.READ_COMMITTED);
            for (String collectionName : committedWrites.keySet()) {
                MongoCollection<Document> c = database.getCollection(collectionName);
                c.deleteMany(
                        Filters.and(
                                Filters.lt(MongoContext.beginVersion, globalxmin),
                                Filters.eq(MongoContext.committed, false)
                        )
                );
            }
        }
    }
}
