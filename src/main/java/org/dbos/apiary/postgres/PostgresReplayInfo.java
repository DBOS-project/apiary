package org.dbos.apiary.postgres;

public class PostgresReplayInfo {

    public final long txnId;
    public final long execId;
    public final long funcId;
    public final String funcName;
    public final String txnSnapshot;

    public PostgresReplayInfo(long txnId, long execId, long funcId, String funcName, String txnSnapshot) {
        this.txnId = txnId;
        this.execId = execId;
        this.funcId = funcId;
        this.funcName = funcName;
        this.txnSnapshot = txnSnapshot;
    }
}
