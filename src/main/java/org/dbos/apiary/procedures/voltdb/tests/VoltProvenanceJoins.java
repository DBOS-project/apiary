package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class VoltProvenanceJoins extends VoltApiaryProcedure {

    public static final SQLStmt addEntry = new SQLStmt(
            // KEY, VALUE
            "UPSERT INTO KVTable VALUES (?, ?);"
    );

    public static final SQLStmt addEntryTwo = new SQLStmt(
            // KEY, VALUE
            "UPSERT INTO KVTableTwo VALUES (?, ?);"
    );

    public static final SQLStmt getValue = new SQLStmt(
            // KEY, VALUE
            "SELECT KVValue, KVValueTwo FROM KVTable Join KVTableTwo ON KVKey = KVKeyTwo;"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(voltInput);
    }

    public int runFunction(ApiaryStatefulFunctionContext ctxt, int key, int value, int valueTwo) {
        ctxt.apiaryExecuteUpdate(addEntry, key, value);
        ctxt.apiaryExecuteUpdate(addEntryTwo, key, valueTwo);
        VoltTable[] vs = (VoltTable[]) ctxt.apiaryExecuteQuery(getValue);
        VoltTable v = vs[0];
        int res1 = (int) v.fetchRow(0).getLong(0);
        int res2 = (int) v.fetchRow(0).getLong(1);
        return res1 + res2;
    }
}
