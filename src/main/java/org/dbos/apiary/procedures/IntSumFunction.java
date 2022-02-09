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
        System.out.println("Executing sum: " + key + " " + str1 + " " + str2);
        int num1 = Integer.valueOf(str1);
        int num2 = Integer.valueOf(str2);
        int sum = num1 + num2;
        voltQueueSQL(addResult, this.pkey, key, sum);
        voltExecuteSQL();
        return String.valueOf(sum);
    }
}
