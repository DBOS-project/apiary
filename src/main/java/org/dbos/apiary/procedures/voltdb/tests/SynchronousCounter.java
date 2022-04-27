package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class SynchronousCounter extends VoltProcedureContainer {

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(voltInput);
    }

    public String runFunction(ApiaryStatefulFunctionContext context, String keyString) {
        int key = Integer.parseInt(keyString);

        VoltTable res = ((VoltTable[]) context.apiaryExecuteQuery(getValue, key))[0];
        int value;
        if (res.getRowCount() > 0) {
            value = (int) res.fetchRow(0).getLong(0);
        } else {
            value = 0;
        }
        String incremented = context.apiaryCallFunction(context, "org.dbos.apiary.procedures.stateless.StatelessIncrement", Integer.toString(value)).getString();
        context.apiaryCallFunction(context, "org.dbos.apiary.procedures.voltdb.tests.InsertFunction", keyString, incremented);
        return incremented;
    }
}
