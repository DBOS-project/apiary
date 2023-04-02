package org.dbos.apiary.worker;

import org.dbos.apiary.ExecuteFunctionRequest;

public class ApiaryNaiveScheduler implements ApiaryScheduler {

    @Override
    public long getPriority(String role, long runtime) {
        return System.nanoTime();
    }

    @Override
    public void onDequeue(ExecuteFunctionRequest req) {

    }
}
