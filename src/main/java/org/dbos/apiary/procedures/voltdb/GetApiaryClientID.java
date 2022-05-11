package org.dbos.apiary.procedures.voltdb;

import org.dbos.apiary.function.ApiaryTransactionalContext;
import org.dbos.apiary.voltdb.VoltFunction;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class GetApiaryClientID extends VoltFunction {
    public final SQLStmt getValue = new SQLStmt (
            "SELECT MetadataValue FROM ApiaryMetadata WHERE MetadataKey=?;"
    );

    public final SQLStmt updateValue = new SQLStmt (
            "UPSERT INTO ApiaryMetadata VALUES (?, ?);"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public int runFunction(ApiaryTransactionalContext context, String key) {
        voltQueueSQL(getValue, key);
        VoltTable results = ((VoltTable[]) voltExecuteSQL())[0];
        int value = results.getRowCount() == 0 ? 0 : (int) results.fetchRow(0).getLong(0);
        voltQueueSQL(updateValue, key, value + 1);
        voltExecuteSQL();
        return value + 1;
    }
}
