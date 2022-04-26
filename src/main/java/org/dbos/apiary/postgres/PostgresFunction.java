package org.dbos.apiary.postgres;

import org.dbos.apiary.interposition.ApiaryFunction;
import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;

public class PostgresFunction implements ApiaryFunction {

    protected ApiaryStatefulFunctionContext context = new PostgresFunctionContext(null);

    @Override
    public ApiaryFunctionContext getContext() {
        return context;
    }

    @Override
    public void setContext(ApiaryFunctionContext context) {
        assert (context instanceof ApiaryStatefulFunctionContext);
        this.context = (ApiaryStatefulFunctionContext) context;
     }
}
