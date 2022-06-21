package org.dbos.apiary.procedures.mongo;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.dbos.apiary.mongo.MongoContext;
import org.dbos.apiary.mongo.MongoFunction;

import java.util.Arrays;
import java.util.List;

public class MongoFindPerson extends MongoFunction {

    public int runFunction(MongoContext context, String name) {
        List<Bson> aggregation = Arrays.asList(
                Aggregates.match(new Document("name", name)),
                Aggregates.count()
        );
        AggregateIterable<Document> found = context.aggregate("people", aggregation);
        Document d = found.first();
        if (d == null) {
            return 0;
        } else {
            return d.getInteger("count");
        }
    }
}
