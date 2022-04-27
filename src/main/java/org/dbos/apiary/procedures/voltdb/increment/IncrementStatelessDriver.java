package org.dbos.apiary.procedures.voltdb.increment;

import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.StatelessFunction;

public class IncrementStatelessDriver extends StatelessFunction {

    public static String runFunction(ApiaryFunctionContext context, Integer key) {
        return context.apiaryCallFunction(context, "IncrementProcedure", key).getString();
    }
}