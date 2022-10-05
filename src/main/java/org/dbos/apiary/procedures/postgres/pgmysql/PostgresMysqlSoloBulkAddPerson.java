package org.dbos.apiary.procedures.postgres.pgmysql;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresMysqlSoloBulkAddPerson extends PostgresFunction {

    public static int runFunction(PostgresContext ctxt, String[] names, int[] numbers) throws Exception {
        ctxt.apiaryCallFunction("MysqlBulkAddPerson", names, numbers);
        return names.length;
    }
}
