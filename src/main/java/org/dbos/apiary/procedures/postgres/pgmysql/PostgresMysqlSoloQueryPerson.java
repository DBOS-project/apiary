package org.dbos.apiary.procedures.postgres.pgmysql;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresMysqlSoloQueryPerson extends PostgresFunction {

    public static int runFunction(PostgresContext ctxt, String search) throws Exception {
        int mysqlCount = ctxt.apiaryCallFunction("MysqlQueryPerson", search).getInt();
       return mysqlCount;
    }
}
