package org.dbos.apiary.executor;

import org.dbos.apiary.interposition.ApiaryFuture;

import java.util.List;

public class FunctionOutput {
    public final Object valueOutput;
    public ApiaryFuture futureOutput;
    public final List<Task> queuedTasks;

    public FunctionOutput(Object valueOutput, ApiaryFuture futureOutput, List<Task> queuedTasks) {
        this.valueOutput = valueOutput;
        this.futureOutput = futureOutput;
        this.queuedTasks = queuedTasks;
    }

    public String getString() {
        assert(valueOutput != null);
        assert (valueOutput instanceof String);
        return (String) valueOutput;
    }

    public int getInt() {
        assert (valueOutput != null);
        assert (valueOutput instanceof Integer);
        return (int) valueOutput;
    }
}
