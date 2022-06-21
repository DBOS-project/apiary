package org.dbos.apiary.procedures.postgres.hotel;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresAddHotel extends PostgresFunction {

    private static final String insert = "INSERT INTO HotelsTable(HotelID, HotelName, AvailableRooms) VALUES (?, ?, ?);";

    public static int runFunction(PostgresContext ctxt, int hotelID, String hotelName, int numRooms)throws Exception {
        ctxt.executeUpdate(insert, hotelID, hotelName, numRooms);
        return hotelID;
    }
}
