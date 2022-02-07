package org.dbos.apiary.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class TruncateTables extends VoltProcedure {
    public final SQLStmt truncateIncrementTable= new SQLStmt(
            "TRUNCATE TABLE IncrementTable;"
    );

    public long run() throws VoltAbortException {
        voltQueueSQL(truncateIncrementTable);
        voltExecuteSQL();
        return 0;
    }
}
