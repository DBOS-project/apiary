package org.dbos.apiary.procedures.mongo.hotel;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.dbos.apiary.mongo.MongoContext;
import org.dbos.apiary.mongo.MongoFunction;

import java.util.ArrayList;
import java.util.List;

public class MongoSearchHotel extends MongoFunction {

    public int[] runFunction(MongoContext context, int longitude, int latitude) {
        Point point = new Point(new Position(longitude, latitude));
        Bson query = Filters.near("point", point, 10000000., 0.);
        FindIterable<Document> ds = context.find("hotels", query);
        List<Integer> hotels = new ArrayList<>();
        int count = 0;
        for (Document d: ds) {
            hotels.add(d.getInteger("hotelID"));
            count++;
            if (count > 5) {
                break;
            }
        }
        return hotels.stream().mapToInt(i -> i).toArray();
    }
}
