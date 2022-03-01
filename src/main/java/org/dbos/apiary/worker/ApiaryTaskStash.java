package org.dbos.apiary.worker;

import org.dbos.apiary.executor.Task;
import org.dbos.apiary.interposition.ApiaryFuture;
import org.zeromq.ZFrame;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

// This class is used to store the current execution progress of a called function.
public class ApiaryTaskStash {
    public final int callerId;
    public final int currTaskId;  // Task ID for itself.
    public final ZFrame replyAddr;
    public final Map<Integer, String> taskIDtoValue;
    public final Queue<Task> queuedFunctions;
    public final AtomicInteger finishedTasks = new AtomicInteger(0);
    public int totalQueuedFunctions;
    public String stringOutput;
    public ApiaryFuture futureOutput;

    public ApiaryTaskStash(int callerId, int currTaskId, ZFrame replyAddr) {
        this.callerId = callerId;
        this.currTaskId = currTaskId;
        this.replyAddr = replyAddr;
        taskIDtoValue = new ConcurrentHashMap<>();
        queuedFunctions = new ConcurrentLinkedQueue<>();
        totalQueuedFunctions = 0;
    }

    // If everything is resolved, then return the string value.
    // Otherwise, return null.
    String getFinalOutput() {
        if (finishedTasks.get() == totalQueuedFunctions) {
            if (stringOutput != null) {
                return stringOutput;
            } else {
                assert (futureOutput != null);
                assert (taskIDtoValue.containsKey(futureOutput.futureID));
                return taskIDtoValue.get(futureOutput.futureID);
            }
        }
        return null;
    }
}
