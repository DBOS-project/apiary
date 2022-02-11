package org.dbos.apiary.voltdb;

import org.dbos.apiary.interposition.ApiaryFunctionInterface;
import org.voltdb.SQLStmt;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class VoltFunctionInterface extends ApiaryFunctionInterface {

    private final VoltApiaryProcedure p;

    public VoltFunctionInterface(VoltApiaryProcedure p) {
        this.p = p;
    }

    @Override
    protected void internalQueueSQL(Object procedure, Object... input) {
        p.voltQueueSQL((SQLStmt) procedure, input);
    }

    @Override
    protected Object internalExecuteSQL() {
        return p.voltExecuteSQL();
    }

    @Override
    protected Object internalRunFunction(Object... input) {
        // Use reflection to find internal runFunction.
        Method functionMethod = getFunctionMethod(p);
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

    private static Method getFunctionMethod(Object o) {
        for (Method m: o.getClass().getDeclaredMethods()) {
            String name = m.getName();
            if (name.equals("runFunction") && Modifier.isPublic(m.getModifiers())) {
                return m;
            }
        }
        return null;
    }
}
