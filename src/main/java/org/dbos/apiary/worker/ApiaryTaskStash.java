package org.dbos.apiary.worker;

import org.dbos.apiary.executor.Task;
import org.dbos.apiary.interposition.ApiaryFuture;
import org.zeromq.ZFrame;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

// This class is used to store the current execution progress of a called function.
public class ApiaryTaskStash {
    public final long callerId;
    public final int currTaskId;  // Task ID for itself.
    public final ZFrame replyAddr;
    public final Map<Integer, String> taskIDtoValue;
    public final Queue<Task> queuedTasks;
    public final AtomicInteger numFinishedTasks = new AtomicInteger(0);
    public final long senderTimestampNano;
    public final String service;

    public int totalQueuedTasks;
    public String stringOutput;
    public ApiaryFuture futureOutput;
    public AtomicBoolean sentOutput = new AtomicBoolean(false);

    public ApiaryTaskStash(String service, long callerId, int currTaskId, ZFrame replyAddr, long senderTimestampNano) {
        this.service = service;
        this.callerId = callerId;
        this.currTaskId = currTaskId;
        this.replyAddr = replyAddr;
        this.senderTimestampNano = senderTimestampNano;
        taskIDtoValue = new ConcurrentHashMap<>();
        queuedTasks = new ConcurrentLinkedQueue<>();
        totalQueuedTasks = 0;
    }

    // If everything is resolved, then return the string value.
    // Otherwise, return null.
    String getFinalOutput() {
        if (numFinishedTasks.get() == totalQueuedTasks) {
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
