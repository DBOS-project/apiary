package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.executor.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ApiaryFunctionContext provides APIs to invoke other functions and run queries.
 */
public abstract class ApiaryFunctionContext {

    private final AtomicInteger calledFunctionID = new AtomicInteger(0);
    private final List<Task> queuedTasks = new ArrayList<>();
    public final ProvenanceBuffer provBuff;
    public final String service;
    public final long execID;
    public final long functionID;

    public ApiaryFunctionContext(ProvenanceBuffer provBuff, String service, long execID, long functionID) {
        this.provBuff = provBuff;
        this.service = service;
        this.execID = execID;
        this.functionID = functionID;
    }

    /** Public Interface for functions. **/

    /**
     * Queue a function for asynchronous execution.
     * This function would synchronously queue the invoked function, but will execute the function asynchronously.
     *
     * @param name      the name of the invoked function.
     * @param inputs    the list of arguments provided to the invoked function.
     * @return          an {@link ApiaryFuture} object that holds the future ID.
     */
    public ApiaryFuture apiaryQueueFunction(String name, Object... inputs) {
        long functionID = ((this.functionID + calledFunctionID.incrementAndGet()) << 4);
        Task futureTask = new Task(functionID, name, inputs);
        queuedTasks.add(futureTask);
        return new ApiaryFuture(functionID);
    }

    /**
     * Invoke a function synchronously and block waiting for the result.
     *
     * @param name      the name of the invoked function.
     * @param inputs    the list of arguments provided to the invoked function.
     * @return          an {@link FunctionOutput} object that stores the output from a function.
     */
    public abstract FunctionOutput apiaryCallFunction(String name, Object... inputs);

    /** Apiary-private **/

    public abstract FunctionOutput checkPreviousExecution();

    public abstract void recordExecution(FunctionOutput output);

    public FunctionOutput getFunctionOutput(Object output) {
        return new FunctionOutput(output, queuedTasks);
    }
}
