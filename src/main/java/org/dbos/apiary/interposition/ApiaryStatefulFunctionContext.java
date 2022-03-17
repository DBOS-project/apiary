package org.dbos.apiary.interposition;

import org.dbos.apiary.utilities.Utilities;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

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
        ApiaryFunction v = (ApiaryFunction) clazz;
        v.setContext(this);
        Method functionMethod = Utilities.getFunctionMethod(v, "runFunction");
        assert functionMethod != null;
        Object output;
        try {
            output = functionMethod.invoke(v, inputs);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return output;
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
