package org.dbos.apiary.worker;

import org.dbos.apiary.ExecuteFunctionRequest;

public interface ApiaryScheduler {
    public long getPriority(String role, long runtime);

    public void onDequeue(ExecuteFunctionRequest req);
}
