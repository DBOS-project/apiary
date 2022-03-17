package org.dbos.apiary.interposition;

import org.dbos.apiary.executor.FunctionOutput;

import java.lang.reflect.InvocationTargetException;

public abstract class ApiaryStatefulFunctionContext extends ApiaryFunctionContext {

    /** Public Interface for functions. **/

    public Object apiaryCallFunction(String name, Object... inputs) {
        // TODO: Logging?
        Object clazz;
        try {
            clazz = Class.forName(name).getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
            return null;
        }
        assert(clazz instanceof ApiaryFunction);
        ApiaryFunction f = (ApiaryFunction) clazz;
        f.setContext(this);
        FunctionOutput o = f.apiaryRunFunction(inputs);
        return o.stringOutput == null ? o.futureOutput : o.stringOutput;
    }

    // Execute an update in the database.
    public void apiaryExecuteUpdate(Object procedure, Object... input) {
        // TODO: Provenance capture.
        internalExecuteUpdate(procedure, input);
    }

    // Execute a database query.
    public Object apiaryExecuteQuery(Object procedure, Object... input) {
        // TODO: Provenance capture.
        return internalExecuteQuery(procedure, input);
    }

    /** Abstract and require implementation. **/
    protected abstract void internalExecuteUpdate(Object procedure, Object... input);
    protected abstract Object internalExecuteQuery(Object procedure, Object... input);

}
