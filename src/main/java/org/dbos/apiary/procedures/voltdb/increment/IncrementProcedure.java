package org.dbos.apiary.procedures.voltdb.increment;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class IncrementProcedure extends VoltApiaryProcedure {

    public final SQLStmt getValue = new SQLStmt (
            "SELECT KVVAlue FROM KVTable WHERE KVKey=?;"
    );

    public final SQLStmt updateValue = new SQLStmt (
            "UPSERT INTO KVTable VALUES (?, ?);"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public int runFunction(ApiaryStatefulFunctionContext context, Integer key) {
        VoltTable results = ((VoltTable[]) context.apiaryExecuteQuery(getValue, key))[0];
        int value = results.getRowCount() == 0 ? 0 : (int) results.fetchRow(0).getLong(0);
        context.apiaryExecuteUpdate(updateValue, key, value + 1);
        return value + 1;
    }

}
