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

    private final ApiaryConnection c;
    private final InternalApiaryWorkerClient client;
    private final Map<String, Callable<StatelessFunction>> statelessFunctions;

    public ApiaryStatelessContext(ApiaryConnection c, InternalApiaryWorkerClient client, ProvenanceBuffer provBuff, String service, long execID, long functionID, Map<String, Callable<StatelessFunction>> statelessFunctions) {
        super(provBuff, service, execID, functionID);
        this.client = client;
        this.statelessFunctions = statelessFunctions;
        this.c = c;
    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) {
        if (statelessFunctions.containsKey(name)) {
            StatelessFunction f = null;
            try {
                f = statelessFunctions.get(name).call();
            } catch (Exception e) {
                e.printStackTrace();
            }
            assert f != null;
            return f.apiaryRunFunction(this, inputs);
        } else {
            try {
                return client.executeFunction(c.getHostname(inputs), name, service, execID, inputs);
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
