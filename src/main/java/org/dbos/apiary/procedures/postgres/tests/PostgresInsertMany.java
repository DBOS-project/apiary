package org.dbos.apiary.procedures.postgres.tests;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.util.ArrayList;
import java.util.List;

public class PostgresInsertMany extends PostgresFunction {

    public static int runFunction(PostgresContext ctxt, String tableName, int numColumns, int[] numbers) throws Exception {
        // Assume the table is a list of numeric (BIGINT) columns.
        StringBuilder query = new StringBuilder(String.format("INSERT INTO %s VALUES (", tableName));
        for (int i = 0; i < numColumns; i++) {
            if (i == 0) {
                query.append("?");
            } else {
                query.append(", ?");
            }
        }
        query.append(");");

        List<Object[]> inputs = new ArrayList<>();
        for (int i = 0; i < numbers.length; i++) {
            Object[] input = new Object[numColumns];
            for (int j = 0; j < numColumns; j++) {
                input[j] = (long) numbers[i];
            }
            inputs.add(input);
        }
        ctxt.insertMany(query.toString(), inputs);
        return numbers.length;
    }
}
