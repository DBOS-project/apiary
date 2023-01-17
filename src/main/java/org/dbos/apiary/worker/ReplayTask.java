package org.dbos.apiary.worker;

import java.sql.Connection;

public class ReplayTask {
    public final long execId;
    public final long funcId;
    public final String funcName;
    public final Object[] inputs;
    public final boolean readOnly;

    public ReplayTask(long execId, long funcId, String funcName, Object[] inputs, boolean readOnly) {
        this.execId = execId;
        this.funcId = funcId;
        this.funcName = funcName;
        this.inputs = inputs;
        this.readOnly = readOnly;
    }
}
