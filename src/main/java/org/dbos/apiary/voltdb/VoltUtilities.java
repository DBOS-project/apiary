package org.dbos.apiary.voltdb;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public class VoltUtilities {
    public static VoltTable.ColumnInfo objectToColumnInfo(int index, Object input) {
        if (input instanceof String) {
            return new VoltTable.ColumnInfo("StringT" + index, VoltType.STRING);
        } else if (input instanceof String[]) {
            return new VoltTable.ColumnInfo("StringArrayT" + index, VoltType.VARBINARY);
        } else if (input instanceof Integer) {
            return new VoltTable.ColumnInfo("IntegerT" + index, VoltType.INTEGER);
        } else if (input instanceof ApiaryFuture) {
            return new VoltTable.ColumnInfo("FutureT" + index, VoltType.SMALLINT);
        } else if (input instanceof ApiaryFuture[]) {
            return new VoltTable.ColumnInfo("FutureArrayT" + index, VoltType.VARBINARY);
        }
        return null;
    }
}
