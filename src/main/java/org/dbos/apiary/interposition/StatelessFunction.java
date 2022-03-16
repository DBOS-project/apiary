package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.utilities.Utilities;

import java.lang.reflect.Method;
import java.util.List;

public class StatelessFunction implements ApiaryFunction {

    private ApiaryFunctionContext context;

    @Override
    public void setContext(ApiaryFunctionContext context) {
        this.context = context;
    }

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
        assert (output instanceof String);
        return new FunctionOutput((String) output, null, List.of());
    }
}
