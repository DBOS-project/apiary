package org.dbos.apiary.procedures;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.interposition.ApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class FibonacciFunction extends ApiaryProcedure {
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

    public Object runFunction(String inputStr) {
        int key = Integer.parseInt(inputStr);
        if (key < 0) {
            return "";
        }
        if (key == 0) {
            voltQueueSQL(addResult, this.pkey, key, 0);
            voltExecuteSQL();
            return "0";
        }
        if (key == 1) {
            voltQueueSQL(addResult, this.pkey, key, 1);
            voltExecuteSQL();
            return "1";
        }
        // Check if the number has been calculated before.
        voltQueueSQL(getValue, key);
        VoltTable res = voltExecuteSQL()[0];
        int val = -1;
        if (res.getRowCount() > 0) {
            val = (int) res.fetchRow(0).getLong(0);
            return String.valueOf(val);
        }

        // Otherwise, call functions.
        ApiaryFuture f1 = callFunction("FibonacciFunction", this.pkey, String.valueOf(key - 2));
        ApiaryFuture f2 = callFunction("FibonacciFunction", this.pkey, String.valueOf(key - 1));
        ApiaryFuture fsum = callFunction("FibSumFunction", this.pkey, key, f1, f2);
        return fsum;
    }
}
