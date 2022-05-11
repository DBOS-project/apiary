package org.dbos.apiary.procedures.voltdb.tests;

import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.StatelessFunction;

public class StatelessIncrement extends StatelessFunction {

    public static String runFunction(ApiaryContext ctxt, String inputString) {
        return String.valueOf(Integer.parseInt(inputString) + 1);
    }
}
