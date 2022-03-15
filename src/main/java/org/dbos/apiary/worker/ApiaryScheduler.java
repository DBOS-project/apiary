package org.dbos.apiary.worker;

import org.dbos.apiary.ExecuteFunctionRequest;

public interface ApiaryScheduler {
    public long getPriority(ExecuteFunctionRequest req);

    public void onDequeue(ExecuteFunctionRequest req);
}
