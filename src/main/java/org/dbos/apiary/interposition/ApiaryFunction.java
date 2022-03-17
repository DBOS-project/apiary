package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.utilities.Utilities;

import java.lang.reflect.Method;

public interface ApiaryFunction {

    ApiaryFunctionContext getContext();

    void setContext(ApiaryFunctionContext context);

    default FunctionOutput apiaryRunFunction(Object... input) {
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
            return getContext().getFunctionOutput((String) output);
        } else {
            assert (output instanceof ApiaryFuture);
            return getContext().getFunctionOutput((ApiaryFuture) output);
        }
    }
}
