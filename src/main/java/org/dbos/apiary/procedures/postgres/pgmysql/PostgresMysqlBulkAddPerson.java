package org.dbos.apiary.procedures.postgres.pgmysql;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresMysqlBulkAddPerson extends PostgresFunction {

    private static final String insert = "INSERT INTO PersonTable(Name, Number) VALUES (?, ?) ON CONFLICT (Name) DO UPDATE SET Number = EXCLUDED.Number;";

    public static int runFunction(PostgresContext ctxt, String[] names, int[] numbers) throws Exception {
        ctxt.apiaryCallFunction("MysqlBulkAddPerson", names, numbers);
        for (int i = 0; i < names.length; i++) {
            ctxt.executeUpdate(insert, names[i], numbers[i]);
        }
        return names.length;
    }
}
