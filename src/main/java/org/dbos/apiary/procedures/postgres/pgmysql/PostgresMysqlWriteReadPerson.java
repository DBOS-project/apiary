package org.dbos.apiary.procedures.postgres.pgmysql;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresMysqlWriteReadPerson extends PostgresFunction {

    public static int runFunction(PostgresContext ctxt, String name, int number) throws Exception {
        return ctxt.apiaryCallFunction("MysqlWriteReadPerson", name, number).getInt();
    }
}
