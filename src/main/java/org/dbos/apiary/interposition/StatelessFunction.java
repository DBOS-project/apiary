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

    @Override
    public FunctionOutput apiaryRunFunction(Object... input) {
        // TODO: Logging.
        // Use reflection to find internal runFunction.
        Method functionMethod = Utilities.getFunctionMethod(this, "runFunction");
        assert functionMethod != null;
        Object output;
        try {
            output = functionMethod.invoke(this, input);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        if (output instanceof String) {
            return context.getFunctionOutput((String) output);
        } else {
            assert (output instanceof ApiaryFuture);
            return context.getFunctionOutput((ApiaryFuture) output);
        }
    }
}
