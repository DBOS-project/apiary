package org.dbos.apiary.procedures.postgres.pgmongo;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresWriteReadPerson extends PostgresFunction {

    public static int runFunction(PostgresContext ctxt, String name, int number) throws Exception {
        return ctxt.apiaryCallFunction("MongoWriteReadPerson", name, number).getInt();
    }
}
