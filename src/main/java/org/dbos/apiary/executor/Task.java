package org.dbos.apiary.executor;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

import java.util.HashMap;
import java.util.Map;

public class Task {
    private static final Logger logger = LoggerFactory.getLogger(Task.class);

    public int taskID;  // Unique ID within an execution.
    public final String funcName;
    public final long pkey;  // Partition to run this task.
    public Object[] input;
    public final Map<Integer, Integer> inputIdxToFutureID = new HashMap<>();  // Map from object index to future task ID.

    // Initialize from user input.
    public Task(int taskID, String funcName, long pkey, Object[] input) {
        this.taskID = taskID;
        this.funcName = funcName;
        this.pkey = pkey;
        this.input = input;
        for (int i = 0; i < input.length; i++) {
            Object o = input[i];
            if (o instanceof ApiaryFuture) {
                int futureID = ((ApiaryFuture) o).creatorID;
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
