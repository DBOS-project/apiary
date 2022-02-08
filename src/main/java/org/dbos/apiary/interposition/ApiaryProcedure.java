package org.dbos.apiary.interposition;

import com.google_voltpatches.common.base.Utf8;
import org.dbos.apiary.utilities.Utilities;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ApiaryProcedure extends VoltProcedure {

    AtomicInteger calledFunctionID = new AtomicInteger(0);

    List<VoltTable> calledFunctionInfo = new ArrayList<>();

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        Object[] input = new Object[voltInput.getColumnCount()];
        VoltTableRow inputRow = voltInput.fetchRow(0);
        for (int i = 0; i < voltInput.getColumnCount(); i++) {
            VoltType t = inputRow.getColumnType(i);
            if (t.equals(VoltType.INTEGER)) {
                input[i] = inputRow.getLong(i);
            } else if (t.equals(VoltType.FLOAT)) {
                input[i] = inputRow.getDouble(i);
            } else if (t.equals(VoltType.STRING)) {
                input[i] = inputRow.getString(i);
            } else if (t.equals(VoltType.VARBINARY)) {
                input[i] = Utilities.byteArrayToStringArray(inputRow.getVarbinary(i));
            }
        }
        Method functionMethod = getFunctionMethod(this);
        assert functionMethod != null;
        VoltTable[] voltOutputs = new VoltTable[calledFunctionInfo.size() + 1];
        Object output = functionMethod.invoke(this, input);
        if (output instanceof String) {
            VoltTable voltOutput = new VoltTable(new VoltTable.ColumnInfo("jsonOutput", VoltType.STRING));
            voltOutput.addRow(output);
            voltOutputs[0] = voltOutput;
        } else {
            return null;
        }
        for (int i = 0; i < calledFunctionInfo.size(); i++) {
            voltOutputs[i + 1] = calledFunctionInfo.get(i);
        }
        return voltOutputs;
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

    public ApiaryFuture callFunction(String name, int pkey, Object... inputs) {
        int myID = calledFunctionID.getAndIncrement();
        return new ApiaryFuture(myID);
    }

}
