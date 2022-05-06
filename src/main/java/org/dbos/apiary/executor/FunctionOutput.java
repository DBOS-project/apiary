package org.dbos.apiary.executor;

import org.dbos.apiary.interposition.ApiaryFuture;

import java.util.List;

public class FunctionOutput {
    public final Object output;
    public final List<Task> queuedTasks;

    public FunctionOutput(Object output, List<Task> queuedTasks) {
        assert(output != null);
        this.output = output;
        this.queuedTasks = queuedTasks;
    }

    public String getString() {
        return output instanceof String ? (String) output : null;
    }

    public Integer getInt() {
        return output instanceof Integer ? (Integer) output : null;
    }

    public String[] getStringArray() { return output instanceof String[] ? (String[]) output : null; }

    public int[] getIntArray() { return output instanceof int[] ? (int[]) output : null; }

    public ApiaryFuture getFuture() {
        return output instanceof ApiaryFuture ? (ApiaryFuture) output : null;
    }
}
