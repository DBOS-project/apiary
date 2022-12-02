package org.dbos.apiary.postgres;

import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.ApiaryFunction;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All Postgres functions should extend this class and implement <code>runFunction</code>.
 */
public class PostgresFunction implements ApiaryFunction {
    private static final Logger logger = LoggerFactory.getLogger(PostgresFunction.class);

    @Override
    public void recordInvocation(ApiaryContext ctxt, String funcName) {
    }
}
