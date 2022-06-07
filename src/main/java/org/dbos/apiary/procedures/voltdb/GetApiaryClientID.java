package org.dbos.apiary.procedures.voltdb;

import org.dbos.apiary.voltdb.VoltContext;
import org.dbos.apiary.voltdb.VoltFunction;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class GetApiaryClientID extends VoltFunction {
    public final SQLStmt getValue = new SQLStmt (
            "SELECT MetadataValue FROM ApiaryMetadata WHERE MetadataKey=?;"
    );

    public final SQLStmt updateValue = new SQLStmt (
            "UPSERT INTO ApiaryMetadata VALUES (0, ?, ?);"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws Exception {
        return super.run(pkey, voltInput);
    }

    private static final String clientIDName = "ClientID";

    public int runFunction(VoltContext context, int pkey) {
        voltQueueSQL(getValue, clientIDName);
        VoltTable results = ((VoltTable[]) voltExecuteSQL())[0];
        int value = results.getRowCount() == 0 ? 0 : (int) results.fetchRow(0).getLong(0);
        voltQueueSQL(updateValue, clientIDName, value + 1);
        voltExecuteSQL();
        return value + 1;
    }
}
