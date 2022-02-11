package org.dbos.apiary.voltdb;

import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class VoltApiaryProcedure extends VoltProcedure {

    protected final VoltFunctionInterface funcApi = new VoltFunctionInterface(this);

    public int pkey;

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        // Reset funcApi state across runs.
        funcApi.reset();
        this.pkey = pkey;
        return (VoltTable[]) funcApi.runFunction(voltInput);
    }
}
