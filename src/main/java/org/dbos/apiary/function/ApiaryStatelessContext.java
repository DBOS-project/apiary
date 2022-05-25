package org.dbos.apiary.function;

import com.google.protobuf.Api;
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
    private final Map<String, ApiaryConnection> connections;
    private final InternalApiaryWorkerClient client;

    public ApiaryStatelessContext(ProvenanceBuffer provBuff, String service, long execID, long functionID,
                                  Map<String, String> functionTypes, Map<String, Callable<ApiaryFunction>> functions,
                                  Map<String, ApiaryConnection> connections,
                                  InternalApiaryWorkerClient client) {
        super(provBuff, service, execID, functionID);
        this.functionTypes = functionTypes;
        this.functions = functions;
        this.connections = connections;
        this.client = client;
    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) {
        String type = functionTypes.get(name);
        if (type.equals(ApiaryConfig.stateless)) {
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
                ApiaryConnection c = connections.get(type);
                ApiaryFunction f = functions.get(name).call();
                return c.callFunction(name, f, provBuff, service, execID, functionID, inputs);
            } catch (Exception e) {
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
