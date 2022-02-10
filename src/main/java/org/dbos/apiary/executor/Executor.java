package org.dbos.apiary.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Executor {
    private static final Logger logger = LoggerFactory.getLogger(Executor.class);

    // Execute the root function and return a single JSON string as the result.
    // TODO: better way to handle partition key, and support multi-partition functions (no pkey).
    public static String executeFunction(ApiaryConnection conn, String funcName, int pkey, Object... rawInput)
            throws Exception {

        // Base ID for this current tasks.
        AtomicInteger functionID = new AtomicInteger(0);
        // This stack stores pending functions. The top one should always have all arguments resolved.
        Stack<Task> taskStack = new Stack<>();
        // This map stores the final return value (String) of each function.
        Map<Integer, String> taskIDtoValue = new ConcurrentHashMap<>();
        // If a task returns a future, map the future's ID to the task's ID for later resolution.
        Map<Integer, Integer> futureIDtoTaskID = new ConcurrentHashMap<>();
        // Push the initial function to stack.
        taskStack.push(new Task(functionID.getAndIncrement(), funcName, pkey, rawInput));

        // Run until the stack is empty.
        while (!taskStack.isEmpty()) {
            // Pop a task to process.
            Task currTask = taskStack.pop();
            currTask.dereferenceFutures(taskIDtoValue);
            // Process input to VoltTable and invoke SP.
            FunctionOutput o = conn.callFunction(functionID.getAndIncrement(), currTask.funcName, currTask.pkey, currTask.input);

            if (o.stringOutput != null) { // Handle a string output.
                taskIDtoValue.put(currTask.taskID, o.stringOutput);
                // Recursively resolve any returned futures referencing this value.
                int ID = currTask.taskID;
                while (futureIDtoTaskID.containsKey(ID)) {
                    int nextID = futureIDtoTaskID.get(ID);
                    assert (!taskIDtoValue.containsKey(nextID));
                    taskIDtoValue.put(nextID, o.stringOutput);
                    ID = nextID;
                }
            } else { // Handle a future output.
                assert(o.futureOutput != null);
                futureIDtoTaskID.put(o.futureOutput.creatorID, currTask.taskID);
            }
            // Push future tasks into the stack. Do this in reverse order because later tasks depend on earlier ones.
            for (int i = o.calledFunctions.size() - 1; i >= 0; i--) {
                taskStack.push(o.calledFunctions.get(i));
            }
        }
        return taskIDtoValue.get(0);
    }
}
