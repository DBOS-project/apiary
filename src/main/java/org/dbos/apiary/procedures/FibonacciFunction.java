package org.dbos.apiary.procedures;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class FibonacciFunction extends VoltApiaryProcedure {
    public final SQLStmt addResult = new SQLStmt(
            // PKEY, KEY, VALUE
            "UPSERT INTO KVTable VALUES (?, ?, ?);"
    );

    public final SQLStmt getValue = new SQLStmt(
            "SELECT KVValue FROM KVTable WHERE KVKey=?;"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public Object runFunction(String strKey) {
        int key = Integer.parseInt(strKey);
        if (key < 0) {
            return "";
        }
        if (key == 0) {
            funcApi.apiaryQueueUpdate(addResult, this.pkey, key, 0);
            funcApi.apiaryExecuteSQL();
            return "0";
        }
        if (key == 1) {
            funcApi.apiaryQueueUpdate(addResult, this.pkey, key, 1);
            funcApi.apiaryExecuteSQL();
            return "1";
        }
        // Check if the number has been calculated before.
        funcApi.apiaryQueueQuery(getValue, key);
        VoltTable res = ((VoltTable[]) funcApi.apiaryExecuteSQL())[0];
        int val = -1;
        if (res.getRowCount() > 0) {
            val = (int) res.fetchRow(0).getLong(0);
            return String.valueOf(val);
        }

        // Otherwise, call functions.
        ApiaryFuture f1 = funcApi.apiaryCallFunction("FibonacciFunction", this.pkey, String.valueOf(key - 2));
        ApiaryFuture f2 = funcApi.apiaryCallFunction("FibonacciFunction", this.pkey, String.valueOf(key - 1));
        ApiaryFuture fsum = funcApi.apiaryCallFunction("FibSumFunction", this.pkey, strKey, f1, f2);
        return fsum;
    }
}
