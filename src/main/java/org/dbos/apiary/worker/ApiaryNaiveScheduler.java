package org.dbos.apiary.worker;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.executor.FunctionOutput;

public class ApiaryNaiveScheduler implements ApiaryScheduler {

    private final ApiaryConnection c;

    public ApiaryNaiveScheduler(ApiaryConnection c) {
        this.c = c;
    }

    @Override
    public FunctionOutput scheduleFunction(String name, Object... inputs) {
        try {
            return c.callFunction(name, inputs);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
