package org.dbos.apiary.procedures.postgres.tests;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgresProvenanceJoins extends PostgresFunction {
    private static final String addEntry = "INSERT INTO KVTable(KVKey, KVValue) VALUES (?, ?) ON CONFLICT (KVKey) DO NOTHING;";
    private static final String addEntryTwo = "INSERT INTO KVTableTwo(KVKeyTwo, KVValueTwo) VALUES (?, ?) ON CONFLICT (KVKeyTwo) DO NOTHING;";
    private static final String getValue = "SELECT KVValue, KVValueTWO FROM KVTable, KVTableTwo WHERE KVKey = KVKeyTwo;";

    public static int runFunction(PostgresContext ctxt, int key, int value, int valueTwo) throws SQLException {
        ctxt.executeUpdate(addEntry, key, value);
        ctxt.executeUpdate(addEntryTwo, key, valueTwo);
        ResultSet rs = ctxt.executeQuery(getValue);
        rs.next();
        return rs.getInt(1) + rs.getInt(2);
    }
}
