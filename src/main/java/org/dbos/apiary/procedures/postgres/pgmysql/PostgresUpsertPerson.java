package org.dbos.apiary.procedures.postgres.pgmysql;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresUpsertPerson extends PostgresFunction {

    private static final String insert = "INSERT INTO PersonTable(Name, Number) VALUES (?, ?) ON CONFLICT (Name) DO UPDATE SET Number = EXCLUDED.Number;";

    public static int runFunction(PostgresContext ctxt, String name, int number) throws Exception {
        ctxt.apiaryCallFunction("MysqlUpsertPerson", name, number);
        ctxt.executeUpdate(insert, name, number);
        return number;
    }
}
