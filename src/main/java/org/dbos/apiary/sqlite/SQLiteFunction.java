package org.dbos.apiary.sqlite;

import org.dbos.apiary.cockroachdb.CockroachDBFunctionContext;
import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.interposition.ApiaryFunction;
import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.utilities.Utilities;

import java.lang.reflect.Method;

public class SQLiteFunction implements ApiaryFunction {

    public ApiaryStatefulFunctionContext context = new CockroachDBFunctionContext();

    @Override
    public void setContext(ApiaryFunctionContext context) {
        this.context = (ApiaryStatefulFunctionContext) context;
    }

    @Override
    public FunctionOutput apiaryRunFunction(Object... input) {
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
