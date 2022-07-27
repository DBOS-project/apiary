package org.dbos.apiary.procedures.mongo.superbenchmark;

import org.bson.Document;
import org.dbos.apiary.mongo.MongoContext;
import org.dbos.apiary.mongo.MongoFunction;
import org.postgresql.util.PSQLException;

public class MongoSBUpdate extends MongoFunction {

    public int runFunction(MongoContext context, int itemID, int cost) throws PSQLException {
        Document item = new Document("itemID", itemID).append("cost", cost);
        context.replaceOne("superbenchmark", item, Integer.toString(itemID));
        return itemID;
    }
}
