package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.executor.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ApiaryStatefulFunction extends ApiaryFunction {

    protected final AtomicInteger calledTaskID = new AtomicInteger(0);
    protected final List<Task> queuedTasks = new ArrayList<>();

    /** Public Interface for functions. **/

    // Asynchronously queue another function for asynchronous execution.
    public ApiaryFuture apiaryQueueFunction(String name, Object... inputs) {
        int taskID = calledTaskID.getAndIncrement();
        Task futureTask = new Task(taskID, name, inputs);
        queuedTasks.add(futureTask);
        return new ApiaryFuture(taskID);
    }

    public Object apiaryCallFunction(String name, Object... inputs) {
        // TODO: Logging?
        return internalCallFunction(name, inputs);
    }

    // Execute an update in the database.
    public void apiaryExecuteUpdate(Object procedure, Object... input) {
        // TODO: Provenance capture.
        internalExecuteUpdate(procedure, input);
    }

    // Execute a database query.
    public Object apiaryExecuteQuery(Object procedure, Object... input) {
        // TODO: Provenance capture.
        return internalExecuteQuery(procedure, input);
    }

    /** Exposed to Apiary callers. **/

    public FunctionOutput runFunction(Object... input) {
        this.calledTaskID.set(0);
        this.queuedTasks.clear();
        // TODO: Log metadata.
        Object retVal = internalRunFunction(input);
        // TODO: Fault tolerance stuff.
        String stringOutput = null;
        ApiaryFuture futureOutput = null;
        if (retVal instanceof String) {
            stringOutput = (String) retVal;
        } else {
            assert (retVal instanceof ApiaryFuture);
            futureOutput = (ApiaryFuture) retVal;
        }
        return new FunctionOutput(stringOutput, futureOutput, this.queuedTasks);
    }

    /** Abstract and require implementation. **/

    protected abstract Object internalCallFunction(String name, Object... inputs);
    protected abstract void internalExecuteUpdate(Object procedure, Object... input);
    protected abstract Object internalExecuteQuery(Object procedure, Object... input);
    protected abstract Object internalRunFunction(Object... input);

}
