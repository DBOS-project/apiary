package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.executor.Task;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ApiaryFunctionInterface {

    private AtomicInteger calledFunctionID = new AtomicInteger(0);
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
        Object output = internalExecuteSQL();
        return output;
    }

    protected abstract void internalQueueSQL(Object procedure, Object... input);
    protected abstract Object internalExecuteSQL();

    public Object runFunction(Object... input) {
        // TODO: Log metadata.
        Object retVal = internalRunFunction(input);
        // TODO: Fault tolerance stuff.
        return retVal;
    }

    // Run user code in the target platform. For example, we use reflection for VoltDB.
    protected abstract Object internalRunFunction(Object... input);

    public List<Task> getCalledFunctions() {
        return this.calledFunctions;
    }

    // Reset internal state.
    public void reset() {
        this.calledFunctionID.set(0);
        this.calledFunctions.clear();
        return;
    }

}
