package org.dbos.apiary.function;

import org.dbos.apiary.utilities.Utilities;

import java.lang.reflect.Method;

/**
 * The base for all Apiary functions. Functions usually inherit as subclasses of it.
 */
public interface ApiaryFunction {
    void recordInvocation(ApiaryContext ctxt, String funcName);

    default FunctionOutput apiaryRunFunction(ApiaryContext ctxt, Object... input) {
        // Check if execution has already occured.
        FunctionOutput prev = ctxt.checkPreviousExecution();
        if (prev != null) {
            return prev;
        }

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
        FunctionOutput fo = ctxt.getFunctionOutput(output);
        ctxt.recordExecution(fo);
        return fo;
    }
}
