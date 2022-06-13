package org.dbos.apiary.procedures.postgres.crossdb;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;

public class PostgresBulkIndexPerson extends PostgresFunction {

    private static final String insert = "INSERT INTO PersonTable(Name, Number) VALUES (?, ?) ON CONFLICT (Name) DO UPDATE SET Number = EXCLUDED.Number;";

    public static int runFunction(PostgresContext ctxt, String[] names, int[] numbers) throws SQLException {
        for (int i = 0; i < names.length; i++) {
            ctxt.executeUpdate(insert, names[i], numbers[i]);
        }
        ctxt.apiaryCallFunction("ElasticsearchBulkIndexPerson", names, numbers);
        return 0;
    }
}
