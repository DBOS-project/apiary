package org.dbos.apiary.interposition;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.worker.ApiaryWorkerClient;

import java.util.Map;
import java.util.concurrent.Callable;

public class ApiaryStatelessFunctionContext extends ApiaryFunctionContext {

    private final ApiaryConnection c;
    private final ApiaryWorkerClient client;
    private final String service;
    private final Map<String, Callable<StatelessFunction>> statelessFunctions;

    public ApiaryStatelessFunctionContext(ApiaryConnection c, ApiaryWorkerClient client, String service, Map<String, Callable<StatelessFunction>> statelessFunctions) {
        this.client = client;
        this.statelessFunctions = statelessFunctions;
        this.c = c;
        this.service = service;
    }

    @Override
    public Object apiaryCallFunction(ApiaryFunctionContext ctxt, String name, Object... inputs) {
        if (statelessFunctions.containsKey(name)) {
            StatelessFunction f = null;
            try {
                f = statelessFunctions.get(name).call();
            } catch (Exception e) {
                e.printStackTrace();
            }
            assert f != null;
            FunctionOutput o = f.apiaryRunFunction(ctxt, inputs);
            return o.stringOutput == null ? o.futureOutput : o.stringOutput;
        } else {
            try {
                return client.executeFunction(c.getHostname(inputs), name, service, inputs);
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
                return null;
            }
        }
    }
}
