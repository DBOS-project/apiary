package org.dbos.apiary.procedures.postgres.pgmongo;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresAddPerson extends PostgresFunction {

    private static final String insert = "INSERT INTO PersonTable(Name, Number) VALUES (?, ?);";

    public static int runFunction(PostgresContext ctxt, String name, int number) throws Exception {
        ctxt.apiaryCallFunction("MongoAddPerson", name, number);
        ctxt.executeUpdate(insert, name, number);
        return number;
    }
}
