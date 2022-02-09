package org.dbos.apiary.procedures;

import org.dbos.apiary.interposition.ApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class IntSumFunction extends ApiaryProcedure {
    public final SQLStmt addResult = new SQLStmt(
            // PKEY, KEY, VALUE
            "UPSERT INTO IncrementTable VALUES (?, ?, ?);"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public String runFunction(Integer key, String str1, String str2) {
        int num1 = Integer.getInteger(str1);
        int num2 = Integer.getInteger(str2);
        int sum = num1 + num2;
        voltQueueSQL(addResult, key, sum);
        voltExecuteSQL();
        return String.valueOf(sum);
    }
}
