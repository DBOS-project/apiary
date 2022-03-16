package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.interposition.ApiaryFunction;

public class RetwisMerge extends ApiaryFunction {

    public String runFunction(String[] inputs) {
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
