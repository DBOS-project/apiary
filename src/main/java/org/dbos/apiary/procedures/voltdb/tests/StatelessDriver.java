package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.ApiaryFuture;
import org.dbos.apiary.function.StatelessFunction;

public class StatelessDriver extends StatelessFunction {

    public static ApiaryFuture runFunction(ApiaryContext context, String inputString) {
        String incrementString = context.apiaryCallFunction("StatelessIncrement", inputString).getString();
        int one = context.apiaryCallFunction("FibonacciFunction", 1).getInt();
        int sum = Integer.parseInt(incrementString) + one;
        return context.apiaryQueueFunction("FibonacciFunction", sum);
    }
}
