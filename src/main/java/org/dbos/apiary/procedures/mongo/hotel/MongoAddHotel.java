package org.dbos.apiary.procedures.mongo.hotel;

import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import org.bson.Document;
import org.dbos.apiary.mongo.MongoContext;
import org.dbos.apiary.mongo.MongoFunction;
import org.postgresql.util.PSQLException;

public class MongoAddHotel extends MongoFunction {

    public int runFunction(MongoContext context, int hotelID, int longitude, int latitude) throws PSQLException {
        Document hotel = new Document("hotelID", hotelID).append("point", new Point(new Position(longitude, latitude)));
        context.insertOne("hotels", hotel, Integer.toString(hotelID));
        return hotelID;
    }
}
