package org.dbos.apiary.voltdb;

import org.dbos.apiary.interposition.ApiaryFunction;
import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.utilities.Utilities;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class VoltFunctionContext extends ApiaryStatefulFunctionContext {

    private final VoltApiaryProcedure p;

    public VoltFunctionContext(VoltApiaryProcedure p) {
        this.p = p;
    }

    @Override
    public Object internalCallFunction(String name, Object... inputs) {
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

    @Override
    protected void internalExecuteUpdate(Object procedure, Object... input) {
        p.voltQueueSQL((SQLStmt) procedure, input);
        p.voltExecuteSQL();
    }

    @Override
    protected VoltTable[] internalExecuteQuery(Object procedure, Object... input) {
        p.voltQueueSQL((SQLStmt) procedure, input);
        return p.voltExecuteSQL();
    }

}
