package org.dbos.apiary.procedures.mongo;

import org.bson.Document;
import org.dbos.apiary.mongo.MongoContext;
import org.dbos.apiary.mongo.MongoFunction;

import java.util.ArrayList;
import java.util.List;

public class MongoBulkAddPerson extends MongoFunction {

    public int runFunction(MongoContext context, String[] names, int[] numbers) {
        List<Document> documents = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < names.length; i++) {
            Document d = new Document("name", names[i]).append("number", numbers[i]);
            documents.add(d);
            ids.add(names[i]);
        }
        context.insertMany("people", documents, ids);
        return 0;
    }
}
