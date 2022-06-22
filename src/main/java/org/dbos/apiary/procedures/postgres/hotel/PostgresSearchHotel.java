package org.dbos.apiary.procedures.postgres.hotel;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresSearchHotel extends PostgresFunction {

    public static int runFunction(PostgresContext ctxt, int longitude, int latitude)throws Exception {
        return ctxt.apiaryCallFunction("MongoSearchHotel", longitude, latitude).getInt();
    }
}
