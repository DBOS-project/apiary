package org.dbos.apiary.function;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ApiaryContext provides APIs to invoke other functions and run queries.
 */
public abstract class ApiaryContext {

    protected final AtomicInteger calledFunctionID = new AtomicInteger(0);
    private final List<Task> queuedTasks = new ArrayList<>();
    /**
     * For internal use only.
     */
    public final WorkerContext workerContext;
    /**
     * For internal use only.
     */
    public final String service;
    /**
     * For internal use only.
     */
    public final long execID, functionID;

    public ApiaryContext(WorkerContext workerContext, String service, long execID, long functionID) {
        this.workerContext = workerContext;
        this.service = service;
        this.execID = execID;
        this.functionID = functionID;
    }

    /** Public Interface for functions. **/

    /**
     * Queue a function for asynchronous execution.
     *
     * @param name      the name of the invoked function.
     * @param inputs    the list of arguments provided to the invoked function.
     * @return          an {@link ApiaryFuture} object.
     */
    public ApiaryFuture apiaryQueueFunction(String name, Object... inputs) {
        long functionID = ((this.functionID + calledFunctionID.incrementAndGet()) << 4);
        Task futureTask = new Task(functionID, name, inputs);
        queuedTasks.add(futureTask);
        return new ApiaryFuture(functionID);
    }

    /**
     * Synchronously invoke a function.
     *
     * @param name      the fully-qualified name of the invoked function.
     * @param inputs    the list of arguments provided to the invoked function.
     * @return          an {@link FunctionOutput} object that stores the output from a function.
     */
    public abstract FunctionOutput apiaryCallFunction(String name, Object... inputs) throws Exception;

    /** Apiary-private **/

    /**
     * For internal use only.
     * @param output    the original output of a function.
     * @return          the finalized output of a function.
     */
    public FunctionOutput getFunctionOutput(Object output) {
        return new FunctionOutput(output, queuedTasks);
    }
}
