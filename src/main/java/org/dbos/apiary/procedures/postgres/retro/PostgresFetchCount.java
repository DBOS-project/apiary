package org.dbos.apiary.procedures.postgres.retro;


import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresFetchCount extends PostgresFunction {
    public static int runFunction(PostgresContext ctxt, int[] subscribers) throws Exception {
        return subscribers.length;
    }
}
