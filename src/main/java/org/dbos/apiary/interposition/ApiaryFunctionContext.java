package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.executor.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ApiaryFunctionContext {

    private final AtomicInteger calledTaskID = new AtomicInteger(0);
    private final List<Task> queuedTasks = new ArrayList<>();

    /** Public Interface for functions. **/

    // Asynchronously queue another function for asynchronous execution.
    public ApiaryFuture apiaryQueueFunction(String name, Object... inputs) {
        int taskID = calledTaskID.getAndIncrement();
        Task futureTask = new Task(taskID, name, inputs);
        queuedTasks.add(futureTask);
        return new ApiaryFuture(taskID);
    }

    /** Apiary-private **/

    public FunctionOutput getFunctionOutput(String stringOutput) {
        return new FunctionOutput(stringOutput, null, queuedTasks);
    }

    public FunctionOutput getFunctionOutput(ApiaryFuture futureOutput) {
        return new FunctionOutput(null, futureOutput, queuedTasks);
    }

    public void reset() {
        this.queuedTasks.clear();
        this.calledTaskID.set(0);
    }
}
