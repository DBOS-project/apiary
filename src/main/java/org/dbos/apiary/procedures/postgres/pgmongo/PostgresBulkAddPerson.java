package org.dbos.apiary.procedures.postgres.pgmongo;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresBulkAddPerson extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(PostgresBulkAddPerson.class);

    private static final String insert = "INSERT INTO PersonTable(Name, Number) VALUES (?, ?) ON CONFLICT (Name) DO UPDATE SET Number = EXCLUDED.Number;";

    public static int runFunction(PostgresContext ctxt, String[] names, int[] numbers) throws Exception {
        long t0 = System.currentTimeMillis();
        ctxt.apiaryCallFunction("MongoBulkAddPerson", names, numbers);
        logger.info("1 {}", System.currentTimeMillis() - t0);
        t0 = System.currentTimeMillis();
        for (int i = 0; i < names.length; i++) {
            ctxt.executeUpdate(insert, names[i], numbers[i]);
        }
        logger.info("2 {}", System.currentTimeMillis() - t0);
        return 0;
    }
}
