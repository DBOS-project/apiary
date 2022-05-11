package org.dbos.apiary.voltdb;

import org.dbos.apiary.function.ApiaryFuture;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

class VoltUtilities {
    public static VoltTable.ColumnInfo objectToColumnInfo(int index, Object input) {
        if (input instanceof String) {
            return new VoltTable.ColumnInfo("StringT" + index, VoltType.STRING);
        } else if (input instanceof String[]) {
            return new VoltTable.ColumnInfo("StringArrayT" + index, VoltType.VARBINARY);
        } else if (input instanceof Integer) {
            return new VoltTable.ColumnInfo("IntegerT" + index, VoltType.INTEGER);
        } else if (input instanceof int[]) {
            return new VoltTable.ColumnInfo("IntegerArrayT" + index, VoltType.VARBINARY);
        } else if (input instanceof ApiaryFuture) {
            return new VoltTable.ColumnInfo("FutureT" + index, VoltType.BIGINT);
        } else if (input instanceof ApiaryFuture[]) {
            return new VoltTable.ColumnInfo("FutureArrayT" + index, VoltType.VARBINARY);
        }
        return null;
    }
}
