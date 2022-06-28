package org.dbos.apiary.procedures.postgres.pgmongo;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresBulkAddPerson extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(PostgresBulkAddPerson.class);

    private static final String insert = "INSERT INTO PersonTable(Name, Number) VALUES (?, ?) ON CONFLICT (Name) DO UPDATE SET Number = EXCLUDED.Number;";

    public static int runFunction(PostgresContext ctxt, String[] names, int[] numbers) throws Exception {
        ctxt.apiaryCallFunction("MongoBulkAddPerson", names, numbers);
        for (int i = 0; i < names.length; i++) {
            ctxt.executeUpdate(insert, names[i], numbers[i]);
        }
        return 0;
    }
}
