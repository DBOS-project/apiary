package org.dbos.apiary.executor;

public interface ApiaryConnection {
    FunctionOutput callFunction(String name, long pkey, Object... inputs) throws Exception;
}
