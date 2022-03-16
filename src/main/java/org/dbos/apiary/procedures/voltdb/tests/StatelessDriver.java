package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.interposition.StatelessFunction;

public class StatelessDriver extends StatelessFunction {

    public ApiaryFuture runFunction(String inputString) {
        return getContext().apiaryQueueFunction("FibonacciFunction", inputString);
    }
}
