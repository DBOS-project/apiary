package org.dbos.apiary.procedures.mongo.hotel;

import org.bson.Document;
import org.dbos.apiary.mongo.MongoContext;
import org.dbos.apiary.mongo.MongoFunction;

public class MongoMakeReservation extends MongoFunction {

    public int runFunction(MongoContext context, int reservationID, int hotelID, int customerID) {
        Document reservation = new Document("reservationID", reservationID).append("hotelID", hotelID).append("customerID", customerID);
        context.insertOne("reservations", reservation, Integer.toString(reservationID));
        return hotelID;
    }
}
