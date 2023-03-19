package org.dbos.apiary.procedures.mongo;

import org.bson.Document;
import org.dbos.apiary.mongo.MongoContext;
import org.dbos.apiary.mongo.MongoFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MongoBulkBenchmark extends MongoFunction {
    private static final Logger logger = LoggerFactory.getLogger(MongoBulkBenchmark.class);

    public int runFunction(MongoContext context, String[] names, int[] numbers) {
        List<Document> documents = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        for (int docIndex = 0; docIndex < names.length; docIndex++) {
            Document d = new Document("string", names[docIndex]);
            for (int num = 0; num < 10; num++) {
                d = d.append("number" + num, numbers[docIndex] + num);
            }
            documents.add(d);
            ids.add(Integer.toString(docIndex));
        }
        context.insertMany("bulkbenchmark", documents, ids);
        return 0;
    }
}
