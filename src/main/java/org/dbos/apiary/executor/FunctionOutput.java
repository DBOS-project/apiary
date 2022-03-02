package org.dbos.apiary.executor;

import org.dbos.apiary.interposition.ApiaryFuture;

import java.util.List;

public class FunctionOutput {
    public final String stringOutput;
    public ApiaryFuture futureOutput;
    public final List<Task> queuedTasks;

    public FunctionOutput(String stringOutput, ApiaryFuture futureOutput, List<Task> queuedTasks) {
        this.stringOutput = stringOutput;
        this.futureOutput = futureOutput;
        this.queuedTasks = queuedTasks;
    }
}
