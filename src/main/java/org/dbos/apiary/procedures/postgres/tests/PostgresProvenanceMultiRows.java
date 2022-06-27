package org.dbos.apiary.procedures.postgres.tests;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;

public class PostgresProvenanceMultiRows extends PostgresFunction {
    private static final String addEntry = "INSERT INTO KVTable(KVKey, KVValue) VALUES (?, ?) ON CONFLICT (KVKey) DO NOTHING;";

    private static final String getMultiValue = "SELECT KVValue, KVKEY FROM KVTable WHERE KVKey IN (?, ?);";

    public static int runFunction(PostgresContext ctxt, int key1, int value1, int key2, int value2) throws Exception {
        // Add an entry for each key, value pair. Then get both values.
        ctxt.executeUpdate(addEntry, key1, value1);
        ctxt.executeUpdate(addEntry, key2, value2);

        // Get both values.
        ResultSet r = ctxt.executeQuery(getMultiValue, key1, key2);
        r.next();
        if (r.getInt(2) == key1) {
            assert (r.getInt(1) == value1);
        } else {
            assert (r.getInt(1) == value2);
        }

        r.next();
        if (r.getInt(2) == key1) {
            assert (r.getInt(1) == value1);
        } else {
            assert (r.getInt(1) == value2);
        }

        // Return value1 + value2.
        return value1 + value2;
    }
}
