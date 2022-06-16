package org.dbos.apiary.procedures.voltdb.increment;

import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.StatelessFunction;

public class IncrementStatelessDriver extends StatelessFunction {

    public static int runFunction(ApiaryContext context, Integer key) throws Exception {
        return context.apiaryCallFunction("IncrementProcedure", key).getInt();
    }
}