package org.dbos.apiary.procedures.stateless;

import org.dbos.apiary.interposition.ApiaryFunction;

public class StatelessIncrement extends ApiaryFunction {

    public String runFunction(String inputString) {
        return String.valueOf(Integer.parseInt(inputString) + 1);
    }
}
