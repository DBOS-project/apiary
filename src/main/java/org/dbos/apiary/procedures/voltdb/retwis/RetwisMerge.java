package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.StatelessFunction;

public class RetwisMerge extends StatelessFunction {

    public static String runFunction(ApiaryContext context, String[] inputs) {
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
