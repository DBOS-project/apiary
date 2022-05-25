package org.dbos.apiary.function;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.client.InternalApiaryWorkerClient;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * ApiaryStatelessContext is a context for stateless functions.
 */
public class ApiaryStatelessContext extends ApiaryContext {

    public ApiaryStatelessContext(ProvenanceBuffer provBuff, String service, long execID, long functionID) {
        super(provBuff, service, execID, functionID);
    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) {
        return null;
    }

    @Override
    public FunctionOutput checkPreviousExecution() {
        return null;
    }

    @Override
    public void recordExecution(FunctionOutput output) {

    }
}
