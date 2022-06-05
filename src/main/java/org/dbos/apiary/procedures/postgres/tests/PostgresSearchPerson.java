package org.dbos.apiary.procedures.postgres.tests;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresSearchPerson extends PostgresFunction {

    public int runFunction(PostgresContext context, String searchText) {
        return context.apiaryCallFunction("ElasticsearchSearchPerson", searchText).getInt();
    }
}
