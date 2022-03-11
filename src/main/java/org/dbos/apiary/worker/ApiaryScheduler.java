package org.dbos.apiary.worker;

import org.dbos.apiary.executor.FunctionOutput;

public interface ApiaryScheduler {
    public FunctionOutput scheduleFunction(String name, Object... inputs);
    public void shutdown();
}
