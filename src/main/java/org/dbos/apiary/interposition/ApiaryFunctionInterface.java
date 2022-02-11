package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.Task;

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
        Object[] parsedInput = internalParseInput(input);
        Object retVal = internalRunFunction(parsedInput);
        // TODO: Fault tolerance stuff.

        // Construct output, object plus a list of futures.
        Object finalRetVal = internalFinalizeOutput(retVal);
        Object[] outputs = new Object[getCalledFunctions().size() + 1];
        outputs[0] = finalRetVal;
        for (int i = 0; i < getCalledFunctions().size(); i++) {
            outputs[i + 1] = internalSerializeFuture(getCalledFunctions().get(i));
        }
        return outputs;
    }

    // Run user code in the target platform. For example, we use reflection for VoltDB.
    protected abstract Object internalRunFunction(Object... input);

    // Parse input from DB dependent format to objects. E.g., VoltTable to object list.
    protected abstract Object[] internalParseInput(Object... input);

    // Parse output from objects to DB dependent format.
    protected abstract Object internalFinalizeOutput(Object output);

    // Serialize task into DB dependent format. E.g., VoltTable.
    protected abstract Object internalSerializeFuture(Task future);

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
