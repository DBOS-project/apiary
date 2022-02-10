package org.dbos.apiary.executor;

public interface ApiaryConnection {
    FunctionOutput callFunction(int executionID, String name, long pkey, Object... inputs) throws Exception;
}
