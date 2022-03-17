package org.dbos.apiary.worker;

import org.dbos.apiary.ExecuteFunctionRequest;

public class ApiaryNaiveScheduler implements ApiaryScheduler {

    @Override
    public long getPriority(ExecuteFunctionRequest req) {
        return System.nanoTime();
    }

    @Override
    public void onDequeue(ExecuteFunctionRequest req) {

    }
}
