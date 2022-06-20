package org.dbos.apiary.procedures.mongo;

import org.bson.Document;
import org.dbos.apiary.mongo.MongoContext;
import org.dbos.apiary.mongo.MongoFunction;

public class MongoFindPerson extends MongoFunction {

    public int runFunction(MongoContext context, String name) {
        Document person = new Document("name", name);
        Document found = context.find("people", person);
        return found.getInteger("number");
    }
}
