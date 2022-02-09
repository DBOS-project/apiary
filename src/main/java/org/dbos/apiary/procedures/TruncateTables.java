package org.dbos.apiary.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class TruncateTables extends VoltProcedure {
    public final SQLStmt truncateKVTable = new SQLStmt(
            "TRUNCATE TABLE KVTable;"
    );

    public long run() throws VoltAbortException {
        voltQueueSQL(truncateKVTable);
        voltExecuteSQL();
        return 0;
    }
}
