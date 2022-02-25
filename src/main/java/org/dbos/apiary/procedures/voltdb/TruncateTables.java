package org.dbos.apiary.procedures.voltdb;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class TruncateTables extends VoltProcedure {
    public final SQLStmt truncateKVTable = new SQLStmt(
            "TRUNCATE TABLE KVTable;"
    );

    public final SQLStmt truncateRetwisPosts = new SQLStmt(
            "TRUNCATE TABLE RetwisPosts;"
    );

    public final SQLStmt truncateRetwisFollowees = new SQLStmt(
            "TRUNCATE TABLE RetwisFollowees;"
    );

    public long run() throws VoltAbortException {
        voltQueueSQL(truncateKVTable);
        voltQueueSQL(truncateRetwisPosts);
        voltQueueSQL(truncateRetwisFollowees);
        voltExecuteSQL();
        return 0;
    }
}
