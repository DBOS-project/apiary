package org.dbos.apiary.cockroachdb;

import org.dbos.apiary.interposition.ApiaryFunction;
import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;

public class CockroachDBFunction implements ApiaryFunction {

    public ApiaryStatefulFunctionContext context = new CockroachDBFunctionContext();

    @Override
    public ApiaryFunctionContext getContext() {
        return context;
    }

    @Override
    public void setContext(ApiaryFunctionContext context) {
        this.context = (ApiaryStatefulFunctionContext) context;
    }
}
