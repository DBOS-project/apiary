package org.dbos.apiary.procedures.postgres.hotel;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class PostgresSearchHotel extends PostgresFunction {

    private static final String get = "SELECT AvailableRooms FROM HotelsTable WHERE HotelID=?";

    public static int[] runFunction(PostgresContext ctxt, int longitude, int latitude)throws Exception {
        int[] hotels = ctxt.apiaryCallFunction("MongoSearchHotel", longitude, latitude).getIntArray();
        List<Integer> availableHotels = new ArrayList<>();
        for (int hotelID : hotels) {
            ResultSet rs = ctxt.executeQuery(get, hotelID);
            if (rs.next()) {
                if (rs.getInt(1) > 0) {
                    availableHotels.add(hotelID);
                }
            }
        }
        return availableHotels.stream().mapToInt(i -> i).toArray();
    }
}
