package org.dbos.apiary.procedures.mongo.hotel;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import org.bson.Document;
import org.dbos.apiary.mongo.MongoContext;
import org.dbos.apiary.mongo.MongoFunction;

public class MongoSearchHotel extends MongoFunction {

    public int runFunction(MongoContext context, int longitude, int latitude) {
        MongoCollection<Document> c = context.database.getCollection("hotels");
        Point point = new Point(new Position(longitude, latitude));
        FindIterable<Document> ds = c.find(Filters.near("point", point, 10000000., 0.));
        Document d = ds.first();
        if (d != null) {
            return d.getInteger("hotelID");
        } else {
            return -1;
        }
    }
}
