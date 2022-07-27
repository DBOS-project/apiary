package org.dbos.apiary.procedures.mongo.superbenchmark;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.dbos.apiary.mongo.MongoContext;
import org.dbos.apiary.mongo.MongoFunction;

public class MongoSBRead extends MongoFunction {

    public int runFunction(MongoContext context, int itemID) {
        Bson query = Filters.eq("itemID", itemID);
        FindIterable<Document> items = context.find("superbenchmark", query);
        if (items.first() != null) {
            return items.first().getInteger("cost");
        } else {
            return -1;
        }
    }
}
