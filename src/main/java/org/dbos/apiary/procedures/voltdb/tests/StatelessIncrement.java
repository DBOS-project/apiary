package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.StatelessFunction;

public class StatelessIncrement extends StatelessFunction {

    public static String runFunction(ApiaryFunctionContext ctxt, String inputString) {
        return String.valueOf(Integer.parseInt(inputString) + 1);
    }
}
