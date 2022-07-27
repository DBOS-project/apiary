package org.dbos.apiary.procedures.mongo.superbenchmark;

import org.bson.Document;
import org.dbos.apiary.mongo.MongoContext;
import org.dbos.apiary.mongo.MongoFunction;
import org.postgresql.util.PSQLException;

import java.util.ArrayList;
import java.util.List;

public class MongoSBBulkWrite extends MongoFunction {

    public int runFunction(MongoContext context, int[] itemIDs, int[] costs) throws PSQLException {
        List<Document> docs = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < itemIDs.length; i++) {
            Document item = new Document("itemID", itemIDs[i]).append("cost", costs[i]);
            docs.add(item);
            ids.add(Integer.toString(itemIDs[i]));
        }
        context.insertMany("superbenchmark", docs, ids);
        return 0;
    }
}
