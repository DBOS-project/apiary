package org.dbos.apiary.voltdb;

import org.dbos.apiary.executor.Task;
import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.utilities.Utilities;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

import java.lang.reflect.InvocationTargetException;

public class VoltApiaryProcedure extends VoltProcedure {

    protected final VoltFunctionInterface funcApi = new VoltFunctionInterface(this);

    public int pkey;

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        // Reset funcApi state across runs.
        funcApi.reset();
        this.pkey = pkey;
        Object[] input = parseVoltInput(voltInput);
        Object output = funcApi.runFunction(input);

        // Serialize output and the list of futures to VoltTable.
        VoltTable[] voltOutputs = finalizeOutput(output);
        return voltOutputs;
    }

    private VoltTable serializeFuture (Task future) {
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
    private VoltTable[] finalizeOutput(Object output) {
        VoltTable[] voltOutputs = new VoltTable[funcApi.getCalledFunctions().size() + 1];
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
        for (int i = 0; i < funcApi.getCalledFunctions().size(); i++) {
            voltOutputs[i + 1] = serializeFuture(funcApi.getCalledFunctions().get(i));
        }
        return voltOutputs;
    }

    private Object[] parseVoltInput(VoltTable voltInput) {
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
}
