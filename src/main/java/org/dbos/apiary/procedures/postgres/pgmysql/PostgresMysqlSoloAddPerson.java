package org.dbos.apiary.procedures.postgres.pgmysql;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresMysqlSoloAddPerson extends PostgresFunction {

    // For inserts only. Not updates.
    public static int runFunction(PostgresContext ctxt, String name, int number) throws Exception {
        ctxt.apiaryCallFunction("MysqlUpsertPerson", name, number);
        return number;
    }
}
