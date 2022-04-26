package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class InsertFunction extends VoltProcedureContainer {

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(voltInput);
    }

    public String runFunction(ApiaryStatefulFunctionContext context, String keyString, String valueString) {
        context.apiaryExecuteUpdate(addResult, Integer.parseInt(keyString), Integer.parseInt(valueString));
        return valueString;
    }
}
