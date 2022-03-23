package org.dbos.apiary.worker;

import org.dbos.apiary.ExecuteFunctionRequest;

public class ApiaryNaiveScheduler implements ApiaryScheduler {

    @Override
    public long getPriority(String service, long runtime) {
        return System.nanoTime();
    }

    @Override
    public void onDequeue(ExecuteFunctionRequest req) {

    }
}
