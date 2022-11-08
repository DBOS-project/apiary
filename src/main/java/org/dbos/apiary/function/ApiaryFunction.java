package org.dbos.apiary.function;

import org.dbos.apiary.utilities.Utilities;

import java.lang.reflect.Method;

/**
 * The base class for Apiary functions.
 */
public interface ApiaryFunction {
    void recordInvocation(ApiaryContext ctxt, String funcName);

    default FunctionOutput apiaryRunFunction(ApiaryContext ctxt, Object... input) throws Exception {
        // Use reflection to find internal runFunction.
        Method functionMethod = Utilities.getFunctionMethod(this, "runFunction");
        assert functionMethod != null;

        // Record invocation message to the buffer.
        if (ctxt.execID != 0) {
            recordInvocation(ctxt, this.getClass().getName());
        }

        Object output;
        Object[] contextInput = new Object[input.length + 1];
        contextInput[0] = ctxt;
        System.arraycopy(input, 0, contextInput, 1, input.length);
        output = functionMethod.invoke(this, contextInput);
        return ctxt.getFunctionOutput(output);
    }
}
