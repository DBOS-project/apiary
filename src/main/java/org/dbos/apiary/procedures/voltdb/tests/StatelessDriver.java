package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.interposition.StatelessFunction;

public class StatelessDriver extends StatelessFunction {

    public ApiaryFuture runFunction(String inputString) {
        String incrementString = (String) getContext().apiaryCallFunction("StatelessIncrement", inputString);
        String one = (String) getContext().apiaryCallFunction("FibonacciFunction", "1");
        String sum = Integer.toString(Integer.parseInt(incrementString) + Integer.parseInt(one));
        return getContext().apiaryQueueFunction("FibonacciFunction", sum);
    }
}
