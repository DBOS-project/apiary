package org.dbos.apiary.voltdb;

import org.dbos.apiary.executor.Task;
import org.dbos.apiary.interposition.ApiaryFunctionInterface;
import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.utilities.Utilities;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

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
    protected VoltTable[] internalExecuteSQL() {
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

    @Override
    protected Object[] internalParseInput(Object... objInput) {
        VoltTable voltInput = (VoltTable) (objInput[0]);
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
        return input;
    }

    @Override
    protected VoltTable[] internalConstructFinalOutput(Object output) {
        VoltTable voltOutput;
        if (output instanceof String) {
            voltOutput = new VoltTable(new VoltTable.ColumnInfo("jsonOutput", VoltType.STRING));
            voltOutput.addRow(output);
        } else if (output instanceof ApiaryFuture) {
            voltOutput = new VoltTable(new VoltTable.ColumnInfo("future", VoltType.SMALLINT));
            voltOutput.addRow(((ApiaryFuture) output).futureID);
        } else {
            System.out.println("Error: Unrecognized output type: " + output.getClass().getName());
            return null;
        }
        VoltTable[] outputs = new VoltTable[getCalledFunctions().size() + 1];
        outputs[0] = voltOutput;
        for (int i = 0; i < getCalledFunctions().size(); i++) {
            outputs[i + 1] = internalSerializeFuture(getCalledFunctions().get(i));
        }
        return outputs;
    }

    @Override
    protected VoltTable internalSerializeFuture(Task future) {
        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[future.input.length + 3];
        columns[0] = new VoltTable.ColumnInfo("name", VoltType.STRING);
        columns[1] = new VoltTable.ColumnInfo("id", VoltType.BIGINT);
        columns[2] = new VoltTable.ColumnInfo("pkey", VoltType.BIGINT);
        for (int i = 0; i < future.input.length; i++) {
            Object input = future.input[i];
            columns[i + 3] = Utilities.objectToColumnInfo(i, input);
        }
        VoltTable v = new VoltTable(columns);
        Object[] row = new Object[v.getColumnCount()];
        row[0] = future.funcName;
        row[1] = future.taskID;
        row[2] = future.pkey;
        for (int i = 0; i < future.input.length; i++) {
            Object input = future.input[i];
            if (input instanceof String) {
                row[i + 3] = input;
            } else if (input instanceof String[]) {
                row[i + 3] = Utilities.stringArraytoByteArray((String[]) input);
            } else if (input instanceof ApiaryFuture) {
                row[i + 3] = ((ApiaryFuture) input).futureID;
            }
        }
        v.addRow(row);
        return v;
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
