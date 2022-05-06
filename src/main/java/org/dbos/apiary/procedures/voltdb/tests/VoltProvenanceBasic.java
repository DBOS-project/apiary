package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class VoltProvenanceBasic extends VoltProcedureContainer {

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(voltInput);
    }

    public int runFunction(ApiaryStatefulFunctionContext ctxt, int key, int baseValue) {
        if (baseValue == 1) {
            ctxt.apiaryExecuteUpdate(addResult, key, baseValue);
            return baseValue+1;
        } else {
            // Synchronously call.
            int res = ctxt.apiaryCallFunction(ctxt, "org.dbos.apiary.procedures.voltdb.tests.VoltProvenanceBasic", key, 1).getInt();
            assert (res == 2);
        }
        // Add an entry at a given key and set to base value, get value, then increase the value by 1.
        // Return the increased value.
        ctxt.apiaryExecuteUpdate(addResult, key, baseValue);
        VoltTable[] vs = (VoltTable[]) ctxt.apiaryExecuteQuery(getValue, key);
        VoltTable v = vs[0];
        assert ((int) v.fetchRow(0).getLong(0) == baseValue);

        return baseValue+1;
    }
}
