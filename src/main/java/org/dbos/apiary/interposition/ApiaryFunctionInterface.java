package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.executor.Task;

import java.util.ArrayList;
import java.util.List;

public abstract class ApiaryFunctionInterface {

    private final List<Task> calledFunctions = new ArrayList<>();

    public ApiaryFuture apiaryCallFunction(String name, int pkey, Object... inputs) {
        return null;
    }

    public void apiaryQueueUpdate(Object procedure, Object... input) {
        // TODO: Provenance capture.
        internalQueueSQL(input);
    }


    public void apiaryQueueQuery(Object procedure, Object... input) {
        // TODO: Provenance capture.
        internalQueueSQL(input);
    }

    public void apiaryExecuteSQL() {
        // TODO: Provenance capture.
        internalExecuteSQL();
    }

    protected abstract void internalQueueSQL(Object procedure, Object... input);
    protected abstract void internalExecuteSQL();

    public Object runFunction(Object... input) {
        // TODO: Log metadata.
        Object retVal = internalRunFunction(input);
        // TODO: Fault tolerance stuff.
        return retVal;
    }

    protected abstract Object internalRunFunction(Object... input);

    List<Task> getCalledFunctions() {
        return this.calledFunctions;
    }
}
