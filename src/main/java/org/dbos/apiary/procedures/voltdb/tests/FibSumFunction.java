package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.function.ApiaryTransactionalContext;
import org.dbos.apiary.voltdb.VoltFunction;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class FibSumFunction extends VoltFunction {
    public final SQLStmt addResult = new SQLStmt(
            // KEY, VALUE
            "UPSERT INTO KVTable VALUES (?, ?);"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public int runFunction(ApiaryTransactionalContext context, int key, int num1, int num2) {
        context.apiaryExecuteUpdate(addResult, key, num1 + num2);
        return num1 + num2;
    }
}
