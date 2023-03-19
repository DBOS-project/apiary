package org.dbos.apiary.procedures.postgres.pgmongo;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresBulkBenchmark extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(PostgresBulkBenchmark.class);

    public static int runFunction(PostgresContext ctxt, String[] names, int[] numbers) throws Exception {
        ctxt.apiaryCallFunction("MongoBulkBenchmark", names, numbers);
        return 0;
    }
}
