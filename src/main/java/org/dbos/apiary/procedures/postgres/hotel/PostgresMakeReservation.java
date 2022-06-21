package org.dbos.apiary.procedures.postgres.hotel;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;

public class PostgresMakeReservation extends PostgresFunction {

    private static final String get = "SELECT AvailableRooms FROM HotelsTable WHERE HotelID=?";
    private static final String update = "UPDATE HotelsTable SET AvailableRooms=? WHERE HotelID=?;";

    public static int runFunction(PostgresContext ctxt, int reservationID, int hotelID, int customerID)throws Exception {
        ResultSet rs = ctxt.executeQuery(get, hotelID);
        if (rs.next()) {
            int availableRooms = rs.getInt(1);
            if (availableRooms > 0) {
                ctxt.executeUpdate(update, availableRooms - 1, hotelID);
                ctxt.apiaryCallFunction("MongoMakeReservation", reservationID, hotelID, customerID);
                return 0;
            }
        }
        return 1;
    }
}
