package org.dbos.apiary.procedures.voltdb;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

import static org.dbos.apiary.utilities.ApiaryConfig.defaultPkey;

public class InferenceFunction extends VoltApiaryProcedure {

    // TODO: replace this with an appropriate data grab
    public final SQLStmt getValue = new SQLStmt(
            "SELECT KVValue FROM KVTable WHERE KVKey=?;"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public ApiaryFuture runFunction(String keyString) {
        int key = Integer.parseInt(keyString);

        /*
        // Grab data and extract from VoltTable
        VoltTable res = ((VoltTable[]) funcApi.apiaryExecuteQuery(getValue, key))[0];
        int value;
        if (res.getRowCount() > 0) {
            value = (int) res.fetchRow(0).getLong(0);
        } else {
            value = 0;
        }
        */

        String message = String.valueOf(key);

        // Queue stateless external function
        ApiaryFuture incrementedValue = funcApi.apiaryQueueFunction("infer", defaultPkey, message);
        
        // Queue insertion back into DB
        // funcApi.apiaryQueueFunction("InsertFunction", key, keyString, incrementedValue);
        
        return incrementedValue;
    }
}
