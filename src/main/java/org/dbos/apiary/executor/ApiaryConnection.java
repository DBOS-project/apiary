package org.dbos.apiary.executor;

public interface ApiaryConnection {
    FunctionOutput callFunction(String name, int pkey, Object... inputs) throws Exception;
}
