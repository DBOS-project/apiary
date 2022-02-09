package org.dbos.apiary.executor;

import org.dbos.apiary.context.ApiaryContext;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ProcCallException;

import java.io.IOException;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Executor {
    private static final Logger logger = LoggerFactory.getLogger(Executor.class);

    private static VoltTable inputToVoltTable(Object... rawInput) {
        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[rawInput.length];
        for (int i = 0; i < rawInput.length; i++) {
            Object input = rawInput[i];
            columns[i] = Utilities.objectToColumnInfo(i, input);
        }
        VoltTable v = new VoltTable(columns);
        Object[] row = new Object[v.getColumnCount()];
        for (int i = 0; i < rawInput.length; i++) {
            Object input = rawInput[i];
            if (input instanceof String[]) {
                row[i] = Utilities.stringArraytoByteArray((String[]) input);
            } else if (input instanceof Integer) {
                row[i] = input;
            } else if (input instanceof Double) {
                row[i] = input;
            } else if (input instanceof String) {
                row[i] = input;
            } else {
                logger.error("Do not support input type: {}, in parameter index {}", input.getClass().getName(), i);
                return null;
            }
        }
        v.addRow(row);
        return v;
    }

    // Execute the root function and return a single JSON string as the result.
    // TODO: better way to handle partition key, and support multi-partition functions (no pkey).
    public static String executeFunction(ApiaryContext ctxt, String funcName, int pkey, Object... rawInput)
            throws  IOException, ProcCallException {

        // Base ID for this current tasks.
        AtomicInteger baseTaskID = new AtomicInteger(0);
        // This stack stores pending functions. The top one should always have all arguments resolved.
        Stack<Task> taskStack = new Stack<>();
        // This map stores the final return value (String) of each function.
        Map<Integer, String> taskIDtoValue = new ConcurrentHashMap<>();
        // If a task returns a future, map the future's ID to the task's ID for later resolution.
        Map<Integer, Integer> futureIDtoTaskID = new ConcurrentHashMap<>();
        // Push the initial function to stack.
        taskStack.push(new Task(baseTaskID.getAndIncrement(), funcName, pkey, rawInput));

        // Run until the stack is empty.
        while (!taskStack.isEmpty()) {
            // Pop a task to process.
            Task currTask = taskStack.pop();
            if (!currTask.inputIdxToFutureID.isEmpty()) {
                // Resolve the future reference.
                currTask.dereferenceFutures(taskIDtoValue);
            }
            // Process input to VoltTable and invoke SP.
            VoltTable voltInput = inputToVoltTable(currTask.input);

            VoltTable[] res  = ctxt.client.callProcedure(currTask.funcName, currTask.pkey, voltInput).getResults();
            assert (res.length >= 1);

            int currBase = baseTaskID.getAndAdd(res.length);
            VoltTable retVal = res[0];
            assert (retVal.getColumnCount() == 1 && retVal.getRowCount() == 1);
            if (retVal.getColumnType(0).equals(VoltType.STRING)) {
                String taskOutput = retVal.fetchRow(0).getString(0);
                taskIDtoValue.put(currTask.taskID, taskOutput);
                int ID = currTask.taskID;
                while (futureIDtoTaskID.containsKey(ID)) {
                    int nextID = futureIDtoTaskID.get(ID);
                    assert (!taskIDtoValue.containsKey(nextID));
                    taskIDtoValue.put(nextID, taskOutput);
                    ID = nextID;
                }
            } else {
                assert (retVal.getColumnType(0).equals(VoltType.SMALLINT));
                int futureID = currBase + (int) retVal.fetchRow(0).getLong(0);
                futureIDtoTaskID.put(futureID, currTask.taskID);
            }
            // Push future tasks into the stack, from end to start because a later task depends on prior ones.
            for (int i = res.length - 1; i > 0; i--) {
                Task futureTask = new Task(currBase, res[i]);
                taskStack.push(futureTask);
            }
        }
        return taskIDtoValue.get(0);
    }
}
