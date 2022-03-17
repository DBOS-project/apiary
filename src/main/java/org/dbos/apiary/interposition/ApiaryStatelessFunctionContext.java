package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.worker.ApiaryWorkerClient;

import java.util.Map;
import java.util.concurrent.Callable;

public class ApiaryStatelessFunctionContext extends ApiaryFunctionContext {

    private final ApiaryWorkerClient client;
    private final Map<String, Callable<StatelessFunction>> statelessFunctions;

    public ApiaryStatelessFunctionContext(ApiaryWorkerClient client, Map<String, Callable<StatelessFunction>> statelessFunctions) {
        this.client = client;
        this.statelessFunctions = statelessFunctions;
    }

    @Override
    public Object apiaryCallFunction(String name, Object... inputs) {
        if (statelessFunctions.containsKey(name)) {
            StatelessFunction f = null;
            try {
                f = statelessFunctions.get(name).call();
            } catch (Exception e) {
                e.printStackTrace();
            }
            assert f != null;
            f.setContext(this);
            FunctionOutput o = f.apiaryRunFunction(inputs);
            return o.stringOutput == null ? o.futureOutput : o.stringOutput;
        } else {
            assert(false);
            return null;
        }
    }
}
