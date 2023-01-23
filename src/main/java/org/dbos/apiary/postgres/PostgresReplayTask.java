package org.dbos.apiary.postgres;

import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.worker.ReplayTask;
import zmq.socket.reqrep.Rep;

import java.sql.Connection;

public class PostgresReplayTask {
    public final ReplayTask task;  // Input to run this task.
    public final Connection conn;  // Connection to run this task.
    public FunctionOutput fo;  // Function output of this task.

    public PostgresReplayTask (ReplayTask task, Connection conn) {
        this.task = task;
        this.conn = conn;
        this.fo = null;
    }
}
