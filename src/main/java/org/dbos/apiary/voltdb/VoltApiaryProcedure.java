package org.dbos.apiary.voltdb;

import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.executor.Task;
import org.dbos.apiary.interposition.ApiaryFunction;
import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.utilities.Utilities;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

import java.lang.reflect.InvocationTargetException;

public class VoltApiaryProcedure extends VoltProcedure implements ApiaryFunction {

    public VoltTable[] run(VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        Object[] parsedInput = parseInput(voltInput);
        FunctionOutput output = apiaryRunFunction(new VoltFunctionContext(this), parsedInput);
        return serializeOutput(output);
    }

    /** Private static helper functions. **/

    private static Object[] parseInput(VoltTable voltInput) {
        Object[] input = new Object[voltInput.getColumnCount()];
        VoltTableRow inputRow = voltInput.fetchRow(0);
        for (int i = 0; i < voltInput.getColumnCount(); i++) {
            VoltType t = inputRow.getColumnType(i);
            if (t.equals(VoltType.STRING)) {
                input[i] = inputRow.getString(i);
            } else if (t.equals(VoltType.VARBINARY)) {
                input[i] = Utilities.byteArrayToStringArray(inputRow.getVarbinary(i));
            } else if (t.equals(VoltType.INTEGER)) {
                input[i] = (int) inputRow.getLong(i);
            } else {
                System.out.println("Error: Unrecognized input type: " + t.getName());
            }
        }
        return input;
    }

    private static VoltTable[] serializeOutput(FunctionOutput output) {
        VoltTable voltOutput;
        assert(output.output != null);
        if (output.output instanceof String) {
            voltOutput = new VoltTable(new VoltTable.ColumnInfo("stringOutput", VoltType.STRING));
            voltOutput.addRow(output.output);
        } else if (output.output instanceof Integer || output.output instanceof Long) {
            voltOutput = new VoltTable(new VoltTable.ColumnInfo("intOutput", VoltType.INTEGER));
            voltOutput.addRow(output.output);
        } else if (output.output instanceof ApiaryFuture) {
            ApiaryFuture futureOutput = (ApiaryFuture) output.output;
            voltOutput = new VoltTable(new VoltTable.ColumnInfo("future", VoltType.SMALLINT));
            voltOutput.addRow(futureOutput.futureID);
        } else {
            throw new RuntimeException();
        }
        VoltTable[] outputs = new VoltTable[output.queuedTasks.size() + 1];
        outputs[0] = voltOutput;
        for (int i = 0; i < output.queuedTasks.size(); i++) {
            outputs[i + 1] = serializeTask(output.queuedTasks.get(i));
        }
        return outputs;
    }

    private static VoltTable serializeTask(Task task) {
        int offset = 2;
        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[task.input.length + offset];
        columns[0] = new VoltTable.ColumnInfo("name", VoltType.STRING);
        columns[1] = new VoltTable.ColumnInfo("id", VoltType.BIGINT);
        for (int i = 0; i < task.input.length; i++) {
            Object input = task.input[i];
            columns[i + offset] = VoltUtilities.objectToColumnInfo(i, input);
        }
        VoltTable v = new VoltTable(columns);
        Object[] row = new Object[v.getColumnCount()];
        row[0] = task.funcName;
        row[1] = task.taskID;
        for (int i = 0; i < task.input.length; i++) {
            Object input = task.input[i];
            if (input instanceof String) {
                row[i + offset] = input;
            } else if (input instanceof String[]) {
                row[i + offset] = Utilities.stringArraytoByteArray((String[]) input);
            } else if (input instanceof Integer) {
                row[i + offset] = input;
            } else if (input instanceof ApiaryFuture) {
                row[i + offset] = ((ApiaryFuture) input).futureID;
            } else if (input instanceof ApiaryFuture[]) {
                ApiaryFuture[] futures = (ApiaryFuture[]) input;
                int[] futureIDs = new int[futures.length];
                for (int j = 0; j < futures.length; j++) {
                    futureIDs[j] = futures[j].futureID;
                }
                row[i + offset] = Utilities.intArrayToByteArray(futureIDs);
            }
        }
        v.addRow(row);
        return v;
    }

    @Override
    public void recordInvocation(ApiaryFunctionContext ctxt, String funcName) {
        // TODO: implement this.
    }
}
