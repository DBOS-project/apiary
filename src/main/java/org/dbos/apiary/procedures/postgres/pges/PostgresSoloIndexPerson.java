package org.dbos.apiary.procedures.postgres.pges;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresSoloIndexPerson extends PostgresFunction {

    public static int runFunction(PostgresContext ctxt, String name, int number) throws Exception {
        ctxt.apiaryCallFunction("ElasticsearchIndexPerson", name, number);
        return number;
    }
}
