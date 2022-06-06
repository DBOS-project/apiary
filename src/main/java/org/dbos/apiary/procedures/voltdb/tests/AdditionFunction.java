package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.voltdb.VoltContext;
import org.dbos.apiary.voltdb.VoltFunction;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class AdditionFunction extends VoltFunction {

    public VoltTable[] run(int pkey, VoltTable voltInput) throws Exception {
        return super.run(pkey, voltInput);
    }

    public String runFunction(VoltContext context, Integer one, String two, String[] strings, int[] ints) {
        long sum = one + Integer.parseInt(two) + ints[0] + ints[1];
        StringBuilder sb = new StringBuilder(Long.toString(sum));
        for (String s: strings) {
            sb.append(s);
        }
        return sb.toString();
    }
}
