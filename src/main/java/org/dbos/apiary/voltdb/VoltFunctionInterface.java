package org.dbos.apiary.voltdb;

import org.dbos.apiary.interposition.ApiaryStatefulFunction;
import org.dbos.apiary.interposition.ApiaryFunction;
import org.dbos.apiary.utilities.Utilities;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class VoltFunctionInterface extends ApiaryStatefulFunction {

    private final VoltApiaryProcedure p;

    public VoltFunctionInterface(VoltApiaryProcedure p) {
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
        if (clazz instanceof VoltApiaryProcedure) {
            VoltApiaryProcedure v = (VoltApiaryProcedure) clazz;
            v.funcApi = this;
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
        } else if (clazz instanceof ApiaryFunction) {
            ApiaryFunction s = (ApiaryFunction) clazz;
            return s.runFunction(inputs).stringOutput;
        } else {
            new Exception().printStackTrace();
            return null;
        }
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

    @Override
    protected Object internalRunFunction(Object... input) {
        // Use reflection to find internal runFunction.
        Method functionMethod = Utilities.getFunctionMethod(p, "runFunction");
        assert functionMethod != null;
        Object output;
        try {
            output = functionMethod.invoke(p, input);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return output;
    }
}
