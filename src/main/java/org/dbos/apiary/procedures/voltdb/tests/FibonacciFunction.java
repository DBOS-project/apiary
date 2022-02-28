package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class FibonacciFunction extends VoltApiaryProcedure {

    public final SQLStmt addResult = new SQLStmt(
            // PKEY, KEY, VALUE
            "UPSERT INTO KVTable VALUES (?, ?);"
    );

    public final SQLStmt getValue = new SQLStmt(
            "SELECT KVValue FROM KVTable WHERE KVKey=?;"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(voltInput);
    }

    public Object runFunction(String strKey) {
        int key = Integer.parseInt(strKey);
        if (key < 0) {
            return "";
        }
        if (key == 0) {
            funcApi.apiaryExecuteUpdate(addResult, key, 0);
            return "0";
        }
        if (key == 1) {
            funcApi.apiaryExecuteUpdate(addResult, key, 1);
            return "1";
        }
        // Check if the number has been calculated before.
        VoltTable res = ((VoltTable[]) funcApi.apiaryExecuteQuery(getValue, key))[0];
        if (res.getRowCount() > 0) {
            return String.valueOf(res.fetchRow(0).getLong(0));
        }

        // Otherwise, call functions.
        ApiaryFuture f1 = funcApi.apiaryQueueFunction("FibonacciFunction", String.valueOf(key - 2));
        ApiaryFuture f2 = funcApi.apiaryQueueFunction("FibonacciFunction", String.valueOf(key - 1));
        return funcApi.apiaryQueueFunction("FibSumFunction", strKey, f1, f2);
    }
}
