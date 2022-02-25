package org.dbos.apiary.procedures.voltdb.tests;

import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class SynchronousCounter extends VoltProcedureContainer {

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public String runFunction(String keyString) {
        int key = Integer.parseInt(keyString);

        VoltTable res = ((VoltTable[]) funcApi.apiaryExecuteQuery(getValue, key))[0];
        int value;
        if (res.getRowCount() > 0) {
            value = (int) res.fetchRow(0).getLong(0);
        } else {
            value = 0;
        }
        String incremented = (String) funcApi.apiaryCallFunction("org.dbos.apiary.procedures.stateless.Increment", key, Integer.toString(value));
        funcApi.apiaryCallFunction("org.dbos.apiary.procedures.voltdb.tests.InsertFunction", key, keyString, incremented);
        return incremented;
    }
}
