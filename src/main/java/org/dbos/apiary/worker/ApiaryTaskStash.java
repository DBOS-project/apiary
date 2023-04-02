package org.dbos.apiary.worker;

import org.dbos.apiary.function.ApiaryFuture;
import org.dbos.apiary.function.Task;
import org.zeromq.ZFrame;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

// This class is used to store the current execution progress of a called function.
public class ApiaryTaskStash {
    public final long callerId;
    public final long functionID;  // Task ID for itself.
    public final ZFrame replyAddr;
    public final Map<Long, Object> functionIDToValue;
    public final Queue<Task> queuedTasks;
    public final AtomicInteger numFinishedTasks = new AtomicInteger(0);
    public final long senderTimestampNano;
    public final String role;
    public final long execId;
    public final int replayMode;

    public int totalQueuedTasks;
    public Object output;
    public String errorMsg;

    public ApiaryTaskStash(String role, long execId, long callerId, long functionID, int replayMode, ZFrame replyAddr, long senderTimestampNano) {
        this.role = role;
        this.execId = execId;
        this.callerId = callerId;
        this.functionID = functionID;
        this.replayMode = replayMode;
        this.replyAddr = replyAddr;
        this.senderTimestampNano = senderTimestampNano;
        functionIDToValue = new ConcurrentHashMap<>();
        queuedTasks = new ConcurrentLinkedQueue<>();
        totalQueuedTasks = 0;
        errorMsg = "";
    }

    // If everything is resolved, then return the string value.
    // Otherwise, return null.
    Object getFinalOutput() {
        if (numFinishedTasks.get() == totalQueuedTasks) {
            if (output instanceof ApiaryFuture) {
                ApiaryFuture futureOutput = (ApiaryFuture) output;
                assert (functionIDToValue.containsKey(futureOutput.futureID));
                return functionIDToValue.get(futureOutput.futureID);
            } else {
                return output;
            }
        }
        return null;
    }
}
