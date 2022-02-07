package org.dbos.apiary.interposition;

import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public abstract class ApiaryProcedure extends VoltProcedure {

    public VoltTable[] run(String jsonInput) {
        Object output = runFunction(jsonInput);
        if (output instanceof String) {
            return new VoltTable[]{new VoltTable(new VoltTable.ColumnInfo("jsonOutput", VoltType.STRING))};
        } else {
            return null;
        }
    }

    public abstract Object runFunction(String jsonInput);

    public ApiaryFuture callFunction(String name, Object... args) {
        return null;
    }

}
