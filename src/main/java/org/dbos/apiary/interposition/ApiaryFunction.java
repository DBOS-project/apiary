package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.utilities.Utilities;

import java.lang.reflect.Method;

public interface ApiaryFunction {
    void recordInvocation(ApiaryFunctionContext ctxt, String funcName);

    default FunctionOutput apiaryRunFunction(ApiaryFunctionContext ctxt, Object... input) {
        // Use reflection to find internal runFunction.
        Method functionMethod = Utilities.getFunctionMethod(this, "runFunction");
        assert functionMethod != null;

        // Record invocation message to the buffer.
        recordInvocation(ctxt, this.getClass().getName());

        Object output;
        Object[] contextInput = new Object[input.length + 1];
        contextInput[0] = ctxt;
        System.arraycopy(input, 0, contextInput, 1, input.length);
        try {
            output = functionMethod.invoke(this, contextInput);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return ctxt.getFunctionOutput(output);
    }
}
