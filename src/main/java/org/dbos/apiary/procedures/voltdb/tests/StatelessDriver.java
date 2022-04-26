package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.interposition.StatelessFunction;

public class StatelessDriver extends StatelessFunction {

    public static ApiaryFuture runFunction(ApiaryFunctionContext context, String inputString) {
        String incrementString = (String) context.apiaryCallFunction(context, "StatelessIncrement", inputString);
        String one = (String) context.apiaryCallFunction(context, "FibonacciFunction", "1");
        String sum = Integer.toString(Integer.parseInt(incrementString) + Integer.parseInt(one));
        return context.apiaryQueueFunction("FibonacciFunction", sum);
    }
}
