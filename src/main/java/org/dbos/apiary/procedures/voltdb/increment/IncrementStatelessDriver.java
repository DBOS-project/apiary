package org.dbos.apiary.procedures.voltdb.increment;

import org.dbos.apiary.interposition.StatelessFunction;

public class IncrementStatelessDriver extends StatelessFunction {

    public String runFunction(String inputString) {
        // Pre-process the key to a new key.
        String newKey = Integer.toString(Integer.parseInt(inputString) + 1);
        String result = (String) getContext().apiaryCallFunction("IncrementProcedure", newKey);
        return result;
    }
}