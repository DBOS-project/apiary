package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.executor.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ApiaryFunctionInterface {

    private final AtomicInteger calledFunctionID = new AtomicInteger(0);
    private final List<Task> calledFunctions = new ArrayList<>();

    // Asynchronously queue another function for asynchronous execution.
    public ApiaryFuture apiaryQueueFunction(String name, int pkey, Object... inputs) {
        int taskID = calledFunctionID.getAndIncrement();
        Task futureTask = new Task(taskID, name, pkey, inputs);
        calledFunctions.add(futureTask);
        return new ApiaryFuture(taskID);
    }

    public void apiaryExecuteUpdate(Object procedure, Object... input) {
        // TODO: Provenance capture.
        internalExecuteUpdate(procedure, input);
    }

    public Object apiaryExecuteQuery(Object procedure, Object... input) {
        // TODO: Provenance capture.
        return internalExecuteQuery(procedure, input);
    }

    protected abstract void internalExecuteUpdate(Object procedure, Object... input);
    protected abstract Object internalExecuteQuery(Object procedure, Object... input);

    public FunctionOutput runFunction(Object... input) {
        this.calledFunctionID.set(0);
        this.calledFunctions.clear();
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
        return new FunctionOutput(stringOutput, futureOutput, this.calledFunctions);
    }

    // Run user code in the target platform.
    protected abstract Object internalRunFunction(Object... input);

}
