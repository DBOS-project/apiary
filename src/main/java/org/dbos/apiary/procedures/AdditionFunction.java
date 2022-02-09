package org.dbos.apiary.procedures;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.interposition.ApiaryProcedure;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class AdditionFunction extends ApiaryProcedure {

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public String runFunction(String one, String two, String[] strings) {
        long sum = Integer.parseInt(one) + Integer.parseInt(two);
        StringBuilder sb = new StringBuilder(Long.toString(sum));
        for (String s: strings) {
            sb.append(s);
        }
        return sb.toString();
    }
}
