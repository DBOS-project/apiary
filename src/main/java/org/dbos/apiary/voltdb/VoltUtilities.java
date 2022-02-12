package org.dbos.apiary.voltdb;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public class VoltUtilities {
    public static VoltTable.ColumnInfo objectToColumnInfo(int index, Object input) {
        if (input instanceof String) {
            return new VoltTable.ColumnInfo(Integer.toString(index), VoltType.STRING);
        } else if (input instanceof String[]) {
            return new VoltTable.ColumnInfo(Integer.toString(index), VoltType.VARBINARY);
        } else if (input instanceof ApiaryFuture) {
            return new VoltTable.ColumnInfo(Integer.toString(index), VoltType.SMALLINT);
        }
        return null;
    }
}
