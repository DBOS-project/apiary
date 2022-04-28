package org.dbos.apiary.interposition;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.worker.InternalApiaryWorkerClient;

import java.util.Map;
import java.util.concurrent.Callable;

public class ApiaryStatelessFunctionContext extends ApiaryFunctionContext {

    private final ApiaryConnection c;
    private final InternalApiaryWorkerClient client;
    private final Map<String, Callable<StatelessFunction>> statelessFunctions;

    public ApiaryStatelessFunctionContext(ApiaryConnection c, InternalApiaryWorkerClient client, ProvenanceBuffer provBuff, String service, long execID, Map<String, Callable<StatelessFunction>> statelessFunctions) {
        super(provBuff, service, execID);
        this.client = client;
        this.statelessFunctions = statelessFunctions;
        this.c = c;
    }

    @Override
    public FunctionOutput apiaryCallFunction(ApiaryFunctionContext ctxt, String name, Object... inputs) {
        if (statelessFunctions.containsKey(name)) {
            StatelessFunction f = null;
            try {
                f = statelessFunctions.get(name).call();
            } catch (Exception e) {
                e.printStackTrace();
            }
            assert f != null;
            return f.apiaryRunFunction(ctxt, inputs);
        } else {
            try {
                return client.executeFunction(c.getHostname(inputs), name, service, execID, inputs);
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
                return null;
            }
        }
    }
}
