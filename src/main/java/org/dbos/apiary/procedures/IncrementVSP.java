package org.dbos.apiary.procedures;

import org.voltdb.*;

public class IncrementVSP extends VoltProcedure {
    public final SQLStmt getValue = new SQLStmt (
            "SELECT IncrementValue FROM IncrementTable WHERE IncrementKey=?;"
    );

    public final SQLStmt updateValue = new SQLStmt (
            "UPSERT INTO IncrementTable VALUES (?, ?, ?);"
    );


    public long run(int pkey, VoltTable[] input) throws VoltAbortException {
        long key = input[0].fetchRow(0).getLong(0);
        voltQueueSQL(getValue, key);
        VoltTable results = voltExecuteSQL()[0];

        long value = results.getRowCount() == 0 ? 0 : results.fetchRow(0).getLong(0);
        voltQueueSQL(updateValue, pkey, key, value + 1);
        voltExecuteSQL();

        return value + 1;
    }
}
