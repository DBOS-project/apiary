package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.utilities.Utilities;

import java.lang.reflect.Method;
import java.util.List;

public abstract class ApiaryFunction {

    /** Exposed to Apiary callers. **/

    public FunctionOutput runFunction(Object... input) {
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
