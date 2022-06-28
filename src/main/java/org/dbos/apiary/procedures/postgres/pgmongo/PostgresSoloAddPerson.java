package org.dbos.apiary.procedures.postgres.pgmongo;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresSoloAddPerson extends PostgresFunction {

    public static int runFunction(PostgresContext ctxt, String name, int number) throws Exception {
        ctxt.apiaryCallFunction("MongoAddPerson", name, number);
        return number;
    }
}
