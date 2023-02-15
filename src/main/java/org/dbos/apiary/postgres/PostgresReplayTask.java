package org.dbos.apiary.postgres;

import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.Task;

import java.sql.Connection;
import java.util.concurrent.Future;

public class PostgresReplayTask {
    public final Task task;  // Input to run this task.
    public Connection conn;  // Connection to run this task.
    public FunctionOutput fo;  // Function output of this task.
    public Future<Integer> resFut;  // Task execution future result.
    public long replayTxnID;    // Replay transaction ID.

    public PostgresReplayTask (Task task, Connection conn) {
        this.task = task;
        this.conn = conn;
        this.fo = null;
        this.resFut = null;
        this.replayTxnID = -1; // Need to be updated once we know the transaction ID.
    }
}
