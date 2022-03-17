package org.dbos.apiary.sqlite;

import org.dbos.apiary.cockroachdb.CockroachDBFunctionContext;
import org.dbos.apiary.interposition.ApiaryFunction;
import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;

public class SQLiteFunction implements ApiaryFunction {

    public ApiaryStatefulFunctionContext context = new CockroachDBFunctionContext();

    @Override
    public void setContext(ApiaryFunctionContext context) {
        this.context = (ApiaryStatefulFunctionContext) context;
    }

    @Override
    public ApiaryFunctionContext getContext() {
        return context;
    }
}
