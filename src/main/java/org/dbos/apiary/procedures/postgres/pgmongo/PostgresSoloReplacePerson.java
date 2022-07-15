package org.dbos.apiary.procedures.postgres.pgmongo;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresSoloReplacePerson extends PostgresFunction {

    public static int runFunction(PostgresContext ctxt, String name, int number) throws Exception {
        ctxt.apiaryCallFunction("MongoReplacePerson", name, number);
        return number;
    }
}
