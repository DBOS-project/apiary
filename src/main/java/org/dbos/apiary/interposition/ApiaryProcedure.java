package org.dbos.apiary.interposition;

import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class ApiaryProcedure extends VoltProcedure {

    AtomicInteger calledFunctionID = new AtomicInteger(0);

    public VoltTable[] run(int pkey, String... jsonInput) {
        Object output = runFunction(jsonInput);
        if (output instanceof String) {
            return new VoltTable[]{new VoltTable(new VoltTable.ColumnInfo("jsonOutput", VoltType.STRING))};
        } else {
            return null;
        }
    }

    public abstract Object runFunction(String... jsonInput);

    public ApiaryFuture callFunction(String name, int pkey, String jsonInput, ApiaryFuture... futureInputs) {
        int myID = calledFunctionID.getAndIncrement();
        return new ApiaryFuture(myID);
    }

}
