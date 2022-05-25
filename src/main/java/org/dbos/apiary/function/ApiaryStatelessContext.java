package org.dbos.apiary.function;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.client.InternalApiaryWorkerClient;
import org.dbos.apiary.utilities.ApiaryConfig;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * ApiaryStatelessContext is a context for stateless functions.
 */
public class ApiaryStatelessContext extends ApiaryContext {

    private final Map<String, String> functionTypes;
    private final Map<String, Callable<ApiaryFunction>> functions;
    private final InternalApiaryWorkerClient client;
    
    public ApiaryStatelessContext(ProvenanceBuffer provBuff, String service, long execID, long functionID,
                                  Map<String, String> functionTypes, Map<String, Callable<ApiaryFunction>> functions,
                                  InternalApiaryWorkerClient client) {
        super(provBuff, service, execID, functionID);
        this.functionTypes = functionTypes;
        this.functions = functions;
        this.client = client;
    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) {
        if (functionTypes.get(name).equals(ApiaryConfig.stateless)) {
            ApiaryFunction f = null;
            try {
                f = functions.get(name).call();
            } catch (Exception e) {
                e.printStackTrace();
            }
            assert f != null;
            return f.apiaryRunFunction(this, inputs);
        } else {
            try {
                return client.executeFunction("localhost", name, service, execID, inputs); // TODO: Don't hardcode.
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    @Override
    public FunctionOutput checkPreviousExecution() {
        return null;
    }

    @Override
    public void recordExecution(FunctionOutput output) {

    }
}
