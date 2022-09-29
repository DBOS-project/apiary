package org.dbos.apiary.procedures.mongo;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.dbos.apiary.mongo.MongoContext;
import org.dbos.apiary.mongo.MongoFunction;
import org.dbos.apiary.mongo.MongoUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class MongoFindPerson extends MongoFunction {
    private static final Logger logger = LoggerFactory.getLogger(MongoFindPerson.class);

    public int runFunction(MongoContext context, String name) {
        long t0 = System.nanoTime();
        List<Bson> aggregation = Arrays.asList(
                Aggregates.match(new Document("name", name)),
                Aggregates.count()
        );
        AggregateIterable<Document> found = context.aggregate("people", aggregation);
        Document d = found.first();
        logger.info("Inner: {}", (System.nanoTime() - t0) / 1000);
        if (d == null) {
            return 0;
        } else {
            return d.getInteger("count");
        }
    }
}
