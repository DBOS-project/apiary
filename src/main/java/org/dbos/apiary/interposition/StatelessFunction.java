package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.utilities.Utilities;

import java.lang.reflect.Method;

public class StatelessFunction implements ApiaryFunction {

    private ApiaryFunctionContext context;

    @Override
    public void setContext(ApiaryFunctionContext context) {
        this.context = context;
    }

    @Override
    public ApiaryFunctionContext getContext() {
        return this.context;
    }
}
