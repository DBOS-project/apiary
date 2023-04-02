package org.dbos.apiary.function;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.utilities.ApiaryConfig;

/**
 * ApiaryStatelessContext is a context for stateless functions.
 */
public class ApiaryStatelessContext extends ApiaryContext {

    public ApiaryStatelessContext(WorkerContext workerContext, String role, long execID, long functionID, int replayMode) {
        super(workerContext, role, execID, functionID, replayMode);
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
            try {
                return f.apiaryRunFunction(this, inputs);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } else {
            try {
                assert(type.equals(workerContext.getPrimaryConnectionType()));
                ApiaryConnection c = workerContext.getPrimaryConnection();
                return c.callFunction(name, workerContext, role, execID, functionID, replayMode, inputs);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
    }
}
