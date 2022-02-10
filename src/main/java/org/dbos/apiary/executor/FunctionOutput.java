package org.dbos.apiary.executor;

import org.dbos.apiary.interposition.ApiaryFuture;

import java.util.List;

public class FunctionOutput {
    public final String stringOutput;
    public ApiaryFuture futureOutput;
    public final List<Task> calledFunctions;

    public FunctionOutput(String stringOutput, ApiaryFuture futureOutput, List<Task> calledFunctions) {
        this.stringOutput = stringOutput;
        this.futureOutput = futureOutput;
        this.calledFunctions = calledFunctions;
    }

    public void offsetOutput(int offset) {
        if (futureOutput != null) {
            this.futureOutput = new ApiaryFuture(futureOutput.futureID + offset);
        }
        calledFunctions.forEach(i -> i.offsetIDs(offset));
    }
}
