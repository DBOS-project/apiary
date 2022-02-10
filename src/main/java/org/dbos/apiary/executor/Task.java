package org.dbos.apiary.executor;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Task {
    private static final Logger logger = LoggerFactory.getLogger(Task.class);

    public int taskID;  // Unique ID of this task.
    public final String funcName;
    public final long pkey;  // Partition to run this task.
    public final Object[] input;
    public final Map<Integer, Integer> inputIdxToFutureID = new HashMap<>();  // Map from object index to future task ID.

    // Initialize from user input.
    public Task(int taskID, String funcName, long pkey, Object[] input) {
        this.taskID = taskID;
        this.funcName = funcName;
        this.pkey = pkey;
        this.input = input;
    }

    public void offsetIDs(int offset) {
        taskID = taskID + offset;
        for (int i = 0; i < input.length; i++) {
            Object o = input[i];
            if (o instanceof ApiaryFuture) {
                int futureID = offset + ((ApiaryFuture) o).futureID;
                inputIdxToFutureID.put(i, futureID);
            }
        }
    }

    // Fill out the actual value of the referred future ID.
    public void dereferenceFutures(Map<Integer, String> taskIDtoValue) {
        for (int inputIdx : inputIdxToFutureID.keySet()) {
            int futureID = inputIdxToFutureID.get(inputIdx);
            assert(taskIDtoValue.containsKey(futureID));
            input[inputIdx] = taskIDtoValue.get(futureID);
        }
    }
}
