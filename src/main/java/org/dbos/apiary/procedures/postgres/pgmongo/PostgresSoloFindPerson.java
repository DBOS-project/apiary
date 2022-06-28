package org.dbos.apiary.procedures.postgres.pgmongo;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;

public class PostgresSoloFindPerson extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(PostgresSoloFindPerson.class);

    public static int runFunction(PostgresContext ctxt, String search) throws Exception {
        return ctxt.apiaryCallFunction("MongoFindPerson", search).getInt();
    }
}
