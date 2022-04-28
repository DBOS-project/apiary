package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.executor.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ApiaryFunctionContext {

    private final AtomicInteger calledTaskID = new AtomicInteger(0);
    private final List<Task> queuedTasks = new ArrayList<>();

    public final ProvenanceBuffer provBuff;
    public final String service;
    public final long execID;

    public ApiaryFunctionContext(ProvenanceBuffer provBuff, String service, long execID) {
        this.provBuff = provBuff;
        this.service = service;
        this.execID = execID;
    }

    /** Public Interface for functions. **/

    // Asynchronously queue another function for asynchronous execution.
    public ApiaryFuture apiaryQueueFunction(String name, Object... inputs) {
        int taskID = calledTaskID.getAndIncrement();
        Task futureTask = new Task(taskID, name, inputs);
        queuedTasks.add(futureTask);
        return new ApiaryFuture(taskID);
    }

    public abstract FunctionOutput apiaryCallFunction(ApiaryFunctionContext ctxt, String name, Object... inputs);

    /** Apiary-private **/

    public FunctionOutput getFunctionOutput(Object output) {
        return new FunctionOutput(output, queuedTasks);
    }
}
