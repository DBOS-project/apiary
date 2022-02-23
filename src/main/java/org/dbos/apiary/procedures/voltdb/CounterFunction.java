package org.dbos.apiary.procedures.voltdb;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class CounterFunction extends VoltApiaryProcedure {

    public final SQLStmt getValue = new SQLStmt(
            "SELECT KVValue FROM KVTable WHERE KVKey=?;"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public ApiaryFuture runFunction(String keyString) {
        int key = Integer.parseInt(keyString);

        VoltTable res = ((VoltTable[]) funcApi.apiaryExecuteQuery(getValue, key))[0];
        int value;
        if (res.getRowCount() > 0) {
            value = (int) res.fetchRow(0).getLong(0);
        } else {
            value = 0;
        }
        ApiaryFuture incrementedValue = funcApi.apiaryCallFunction("increment", key, String.valueOf(value));
        funcApi.apiaryCallFunction("InsertFunction", key, keyString, incrementedValue);
        return incrementedValue;
    }
}
