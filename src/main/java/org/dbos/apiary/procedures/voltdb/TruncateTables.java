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

    public final SQLStmt truncateRetwisFollowers = new SQLStmt(
            "TRUNCATE TABLE RetwisFollowers;"
    );

    public long run() throws VoltAbortException {
        voltQueueSQL(truncateKVTable);
        voltQueueSQL(truncateRetwisPosts);
        voltQueueSQL(truncateRetwisFollowers);
        voltExecuteSQL();
        return 0;
    }
}
