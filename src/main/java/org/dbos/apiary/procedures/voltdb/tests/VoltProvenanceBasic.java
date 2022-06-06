package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.voltdb.VoltContext;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class VoltProvenanceBasic extends VoltProcedureContainer {

    public VoltTable[] run(int pkey, VoltTable voltInput) throws Exception {
        return super.run(pkey, voltInput);
    }

    public int runFunction(VoltContext ctxt, int key, int baseValue) {
        if (baseValue == 1) {
            ctxt.executeUpdate(addResult, key, baseValue);
            return baseValue+1;
        } else {
            // Synchronously call.
            int res = ctxt.apiaryCallFunction("org.dbos.apiary.procedures.voltdb.tests.VoltProvenanceBasic", key, 1).getInt();
            assert (res == 2);
        }
        // Add an entry at a given key and set to base value, get value, then increase the value by 1.
        // Return the increased value.
        ctxt.executeUpdate(addResult, key, baseValue);
        VoltTable[] vs = (VoltTable[]) ctxt.executeQuery(getValue, key);
        VoltTable v = vs[0];
        assert ((int) v.fetchRow(0).getLong(0) == baseValue);

        return baseValue+1;
    }
}
