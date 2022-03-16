package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;

public interface ApiaryFunction {
    void setContext(ApiaryFunctionContext context);

    FunctionOutput apiaryRunFunction(Object... input);
}
