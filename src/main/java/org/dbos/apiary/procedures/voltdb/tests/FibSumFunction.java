package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class FibSumFunction extends VoltApiaryProcedure {
    public final SQLStmt addResult = new SQLStmt(
            // PKEY, KEY, VALUE
            "UPSERT INTO KVTable VALUES (?, ?);"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(voltInput);
    }

    public String runFunction(String key, String str1, String str2) {
        int num1 = Integer.parseInt(str1);
        int num2 = Integer.parseInt(str2);
        int sum = num1 + num2;
        funcApi.apiaryExecuteUpdate(addResult, Integer.parseInt(key), sum);
        return String.valueOf(sum);
    }
}
