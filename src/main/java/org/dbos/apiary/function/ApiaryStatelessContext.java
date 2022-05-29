package org.dbos.apiary.function;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.utilities.ApiaryConfig;

/**
 * ApiaryStatelessContext is a context for stateless functions.
 */
public class ApiaryStatelessContext extends ApiaryContext {

    public ApiaryStatelessContext(WorkerContext workerContext, String service, long execID, long functionID) {
        super(workerContext, service, execID, functionID);
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
                return c.callFunction(name, workerContext, service, execID, functionID, inputs);
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
