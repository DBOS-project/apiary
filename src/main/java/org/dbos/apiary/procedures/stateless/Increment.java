package org.dbos.apiary.procedures.stateless;

import org.dbos.apiary.stateless.StatelessFunction;

public class Increment extends StatelessFunction {

    public String runFunction(String inputString) {
        return String.valueOf(Integer.parseInt(inputString) + 1);
    }
}
