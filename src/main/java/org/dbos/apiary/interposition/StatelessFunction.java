package org.dbos.apiary.interposition;

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
