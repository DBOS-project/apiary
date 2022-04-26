package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.interposition.ApiaryStatelessFunctionContext;
import org.dbos.apiary.interposition.StatelessFunction;

public class RetwisMerge extends StatelessFunction {

    public String runFunction(ApiaryStatelessFunctionContext context, String[] inputs) {
        StringBuilder ret = new StringBuilder();
        String sep = "";
        for (String input: inputs) {
            ret.append(sep);
            ret.append(input);
            sep = ",";
        }
        return ret.toString();
    }
}
