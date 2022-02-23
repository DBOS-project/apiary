package org.dbos.apiary.stateless;

import org.dbos.apiary.utilities.Utilities;

import java.lang.reflect.Method;

public class StatelessFunction {

    public String internalRunFunction(Object... input) {
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
        return (String) output;
    }
}
