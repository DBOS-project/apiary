package org.dbos.apiary.procedures.postgres.retro;


import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresFetchCount extends PostgresFunction {
    // Sum up subscriber IDs.
    public static int runFunction(PostgresContext ctxt, int[] subscribers) throws Exception {
        int total = 0;
        for (int i = 0; i < subscribers.length; i++) {
            total += subscribers[i];
        }
        return total;
    }
}
