package org.dbos.apiary.procedures;

import org.dbos.apiary.interposition.ApiaryProcedure;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class AdditionFunction extends ApiaryProcedure {

    public VoltTable run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public String runFunction(Long one, Long two) {
        long sum = one + two;
        return Long.toString(sum);
    }
}
