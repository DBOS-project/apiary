package org.dbos.apiary.executor;

import org.dbos.apiary.context.ApiaryContext;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.VoltTable;
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
        // Push the initial function to stack.
        taskStack.push(new Task(baseTaskID.getAndIncrement(), funcName, pkey, rawInput));
        // What do we return?
        String finalOutput = null;

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
            assert res.length >= 1;

            // The output either contains futures, or contains a String value, but not both.
            // Because if it returns a string value, then the futures are not used.
            // TODO: This isn't necessarily true and we need to change it.
            if (res[0].getColumnCount() == 1) {
                String taskOutput = res[0].fetchRow(0).getString(0);
                taskIDtoValue.put(currTask.taskID, taskOutput);
                if (taskStack.isEmpty()) {
                    // This is the last task, and its output is the final output.
                    finalOutput = taskOutput;
                }
            } else {
                // Push future tasks into the stack, from end to start because a later task depends on prior ones.
                int currBase = baseTaskID.getAndAdd(res.length);
                for (int i = res.length - 1; i >= 0; i--) {
                    Task futureTask = new Task(currBase, res[i]);
                    // If it is the last task, inherit the parent's ID. Otherwise, cannot find the output.
                    // A bit hacky.
                    if (i == res.length - 1) {
                        futureTask.taskID = currTask.taskID;
                    }
                    taskStack.push(futureTask);
                }
            }
        }
        return finalOutput;
    }
}
