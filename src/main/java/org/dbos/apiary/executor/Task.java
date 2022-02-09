package org.dbos.apiary.executor;

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
    public Task(int taskID, String funcName, long pkey, Object... input) {
        this.taskID = taskID;
        this.funcName = funcName;
        this.pkey = pkey;
        this.input = input;
    }

    // Initialize from VoltTable, returned from SP, potentially contains future.
    // Need to take an offset for all IDs. The offset is the base taskID.
    public Task(int baseID, VoltTable voltInput) {
        if (voltInput.getColumnCount() < 3) throw new AssertionError();
        VoltTableRow inputRow = voltInput.fetchRow(0);
        this.funcName = inputRow.getString(0);
        this.taskID = baseID + (int) inputRow.getLong(1);
        this.pkey = inputRow.getLong(2);
        this.input = new Object[voltInput.getColumnCount() - 3];

        int objIndex = 0;
        for (int i = 3; i < voltInput.getColumnCount(); i++, objIndex++) {
            VoltType t = inputRow.getColumnType(i);
            if (t.equals(VoltType.BIGINT)) {
                input[objIndex] = (int) inputRow.getLong(i);
            } else if (t.equals(VoltType.FLOAT)) {
                input[objIndex] = inputRow.getDouble(i);
            } else if (t.equals(VoltType.STRING)) {
                input[objIndex] = inputRow.getString(i);
            } else if (t.equals(VoltType.VARBINARY)) {
                input[objIndex] = Utilities.byteArrayToStringArray(inputRow.getVarbinary(i));
            } else if (t.equals(VoltType.SMALLINT)) {
                input[objIndex] = null;  // Will fill out the actual value later.
                int futureID = baseID + (int) inputRow.getLong(i);
                inputIdxToFutureID.put(objIndex, futureID);
            } else {
                logger.error("Cannot support object type {}, index {}", t.getName(), objIndex);
                throw new IllegalArgumentException();
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
        inputIdxToFutureID.clear();
    }
}
