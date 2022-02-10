package org.dbos.apiary.interposition;

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

    private AtomicInteger calledFunctionID;

    private final List<VoltTable> calledFunctionInfo = new ArrayList<>();

    public int pkey;

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        this.pkey = pkey;
        // TODO: Why this happened? Need to reset, because it seems that VoltDB maintains global state across SP runs.
        //  calledFunctionInfo will continuously grow.
        calledFunctionInfo.clear();
        calledFunctionID = new AtomicInteger(0);
        Object[] input = new Object[voltInput.getColumnCount()];
        VoltTableRow inputRow = voltInput.fetchRow(0);
        for (int i = 0; i < voltInput.getColumnCount(); i++) {
            VoltType t = inputRow.getColumnType(i);
            if (t.equals(VoltType.STRING)) {
                input[i] = inputRow.getString(i);
            } else if (t.equals(VoltType.VARBINARY)) {
                input[i] = Utilities.byteArrayToStringArray(inputRow.getVarbinary(i));
            } else {
                System.out.println("Error: Unrecognized input type: " + t.getName());
            }
        }
        Method functionMethod = getFunctionMethod(this);
        assert functionMethod != null;
        Object output = functionMethod.invoke(this, input);
        VoltTable[] voltOutputs = new VoltTable[calledFunctionInfo.size() + 1];

        if (output instanceof String) {
            VoltTable voltOutput = new VoltTable(new VoltTable.ColumnInfo("jsonOutput", VoltType.STRING));
            voltOutput.addRow(output);
            voltOutputs[0] = voltOutput;
        } else if (output instanceof ApiaryFuture) {
            VoltTable voltOutput = new VoltTable(new VoltTable.ColumnInfo("future", VoltType.SMALLINT));
            voltOutput.addRow(((ApiaryFuture) output).futureID);
            voltOutputs[0] = voltOutput;
        } else {
            System.out.println("Error: Unrecognized output type: " + output.getClass().getName());
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
        int ID = calledFunctionID.getAndIncrement();
        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[inputs.length + 3];
        columns[0] = new VoltTable.ColumnInfo("name", VoltType.STRING);
        columns[1] = new VoltTable.ColumnInfo("id", VoltType.BIGINT);
        columns[2] = new VoltTable.ColumnInfo("pkey", VoltType.BIGINT);
        for (int i = 0; i < inputs.length; i++) {
            Object input = inputs[i];
            columns[i + 3] = Utilities.objectToColumnInfo(i, input);
        }
        VoltTable v = new VoltTable(columns);
        Object[] row = new Object[v.getColumnCount()];
        row[0] = name;
        row[1] = ID;
        row[2] = pkey;
        for (int i = 0; i < inputs.length; i++) {
            Object input = inputs[i];
            if (input instanceof String) {
                row[i + 3] = input;
            } else if (input instanceof String[]) {
                row[i + 3] = Utilities.stringArraytoByteArray((String[]) input);
            } else if (input instanceof ApiaryFuture) {
                row[i + 3] = ((ApiaryFuture) input).futureID;
            }
        }
        v.addRow(row);
        calledFunctionInfo.add(v);
        return new ApiaryFuture(ID);
    }

}
