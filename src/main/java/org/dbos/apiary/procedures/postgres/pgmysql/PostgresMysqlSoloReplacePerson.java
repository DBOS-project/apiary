package org.dbos.apiary.procedures.postgres.pgmysql;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresMysqlSoloReplacePerson extends PostgresFunction {

    // Replace/update person only. Not for inserts.
    public static int runFunction(PostgresContext ctxt, String name, int number) throws Exception {
        ctxt.apiaryCallFunction("MysqlReplacePerson", name, number);
        return number;
    }
}
