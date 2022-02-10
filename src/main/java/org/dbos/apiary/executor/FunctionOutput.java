package org.dbos.apiary.executor;

import org.dbos.apiary.interposition.ApiaryFuture;

import java.util.List;

public class FunctionOutput {
    public final String stringOutput;
    public final ApiaryFuture futureOutput;
    public final List<Task> calledFunctions;

    public FunctionOutput(String stringOutput, ApiaryFuture futureOutput, List<Task> calledFunctions) {
        this.stringOutput = stringOutput;
        this.futureOutput = futureOutput;
        this.calledFunctions = calledFunctions;
    }
}
