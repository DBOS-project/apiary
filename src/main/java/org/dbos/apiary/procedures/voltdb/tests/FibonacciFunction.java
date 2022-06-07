package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.function.ApiaryFuture;
import org.dbos.apiary.voltdb.VoltContext;
import org.dbos.apiary.voltdb.VoltFunction;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class FibonacciFunction extends VoltFunction {

    public final SQLStmt addResult = new SQLStmt(
            // KEY, VALUE
            "UPSERT INTO KVTable VALUES (?, ?);"
    );

    public final SQLStmt getValue = new SQLStmt(
            "SELECT KVValue FROM KVTable WHERE KVKey=?;"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws Exception {
        return super.run(pkey, voltInput);
    }

    public Object runFunction(VoltContext context, int key) {
        if (key < 0) {
            return -1;
        }
        if (key == 0) {
            context.executeUpdate(addResult, key, 0);
            return 0;
        }
        if (key == 1) {
            context.executeUpdate(addResult, key, 1);
            return 1;
        }
        // Check if the number has been calculated before.
        VoltTable res = ((VoltTable[]) context.executeQuery(getValue, key))[0];
        if (res.getRowCount() > 0) {
            return res.fetchRow(0).getLong(0);
        }

        // Otherwise, call functions.
        ApiaryFuture f1 = context.apiaryQueueFunction("FibonacciFunction", key - 2);
        ApiaryFuture f2 = context.apiaryQueueFunction("FibonacciFunction", key - 1);
        return context.apiaryQueueFunction("FibSumFunction", key, f1, f2);
    }
}
