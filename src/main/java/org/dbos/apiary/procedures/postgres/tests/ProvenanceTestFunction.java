package org.dbos.apiary.procedures.postgres.tests;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ProvenanceTestFunction extends PostgresFunction {
    private static final String addEntry = "INSERT INTO KVTable(KVKey, KVValue) VALUES (?, ?) ON CONFLICT (KVKey) DO NOTHING;";
    private static final String getValue = "SELECT KVValue, KVKEY FROM KVTable WHERE KVKey=?;";
    private static final String updateEntry = "UPDATE KVTABLE SET KVvalue=? WHERE KVKEY=?";
    private static final String deleteEntry = "DELETE FROM KVTable WHERE KVKey=?;";

    public static int runFunction(ApiaryStatefulFunctionContext ctxt, int key, int baseValue) throws SQLException {
        // Add an entry at a given key and set to base value, get value, then increase the value by 1, get value again, and finally delete.
        // Return the increased value.
        ctxt.apiaryExecuteUpdate(addEntry, key, baseValue);
        ResultSet r = (ResultSet) ctxt.apiaryExecuteQueryCaptured(getValue, new int[]{2}, key);
        r.next();
        assert (r.getInt(1) == baseValue);

        ctxt.apiaryExecuteUpdate(updateEntry, baseValue+1, key);
        r = (ResultSet) ctxt.apiaryExecuteQueryCaptured(getValue, new int[]{2}, key);
        r.next();
        int res = r.getInt(1);
        assert (res == (baseValue+1));

        ctxt.apiaryExecuteUpdate(deleteEntry, key);
        return res;
    }
}