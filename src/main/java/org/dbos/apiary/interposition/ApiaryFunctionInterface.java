package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ApiaryFunctionInterface {

    private final AtomicInteger calledFunctionID = new AtomicInteger(0);
    private final List<Task> calledFunctions = new ArrayList<>();

    // Asynchronously call another function inside an Apiary function.
    public ApiaryFuture apiaryCallFunction(String name, int pkey, Object... inputs) {
        int taskID = calledFunctionID.getAndIncrement();
        Task futureTask = new Task(taskID, name, pkey, inputs);
        calledFunctions.add(futureTask);
        return new ApiaryFuture(taskID);
    }

    public void apiaryQueueUpdate(Object procedure, Object... input) {
        // TODO: Provenance capture.
        internalQueueSQL(procedure, input);
    }


    public void apiaryQueueQuery(Object procedure, Object... input) {
        // TODO: Provenance capture.
        internalQueueSQL(procedure, input);
    }

    public Object apiaryExecuteSQL() {
        // TODO: Provenance capture.
        return internalExecuteSQL();
    }

    protected abstract void internalQueueSQL(Object procedure, Object... input);
    protected abstract Object internalExecuteSQL();

    public Object runFunction(Object... input) {
        this.calledFunctionID.set(0);
        this.calledFunctions.clear();
        // TODO: Log metadata.
        Object retVal = internalRunFunction(input);
        // TODO: Fault tolerance stuff.
        return retVal;
    }

    // Run user code in the target platform.
    protected abstract Object internalRunFunction(Object... input);

    public List<Task> getCalledFunctions() {
        return this.calledFunctions;
    }
}
