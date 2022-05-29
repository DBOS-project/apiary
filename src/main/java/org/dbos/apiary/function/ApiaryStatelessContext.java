package org.dbos.apiary.function;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.WorkerContext;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * ApiaryStatelessContext is a context for stateless functions.
 */
public class ApiaryStatelessContext extends ApiaryContext {

    private final WorkerContext workerContext;

    public ApiaryStatelessContext(ProvenanceBuffer provBuff, String service, long execID, long functionID,
                                  WorkerContext workerContext) {
        super(provBuff, service, execID, functionID);
        this.workerContext = workerContext;
    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) {
        String type = workerContext.getFunctionType(name);
        if (type.equals(ApiaryConfig.stateless)) {
            ApiaryFunction f = null;
            try {
                f = workerContext.getFunction(name);
            } catch (Exception e) {
                e.printStackTrace();
            }
            assert f != null;
            return f.apiaryRunFunction(this, inputs);
        } else {
            try {
                ApiaryConnection c = workerContext.getConnection(type);
                ApiaryFunction f = workerContext.getFunction(name);
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
