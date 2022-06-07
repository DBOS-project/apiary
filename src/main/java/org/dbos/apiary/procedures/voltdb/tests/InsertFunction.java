package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.voltdb.VoltContext;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class InsertFunction extends VoltProcedureContainer {

    public VoltTable[] run(int pkey, VoltTable voltInput) throws Exception {
        return super.run(pkey, voltInput);
    }

    public String runFunction(VoltContext context, String keyString, String valueString) {
        context.executeUpdate(addResult, Integer.parseInt(keyString), Integer.parseInt(valueString));
        return valueString;
    }
}
