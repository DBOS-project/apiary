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
    public final Object[] input;

    // Initialize from user input.
    public Task(int taskID, String funcName, Object[] input) {
        this.taskID = taskID;
        this.funcName = funcName;
        this.input = input;
    }

    // Fill out the actual value of the referred future ID.
    public void dereferenceFutures(Map<Integer, String> taskIDtoValue) {
        for (int i = 0; i < input.length; i++) {
            Object o = input[i];
            if (o instanceof ApiaryFuture) {
                int futureID = ((ApiaryFuture) o).futureID;
                assert(taskIDtoValue.containsKey(futureID));
                input[i] = taskIDtoValue.get(futureID);
            } else if (o instanceof ApiaryFuture[]) {
                ApiaryFuture[] futureArray = (ApiaryFuture[]) o;
                String[] stringArray = new String[futureArray.length];
                for (int j = 0; j < futureArray.length; j++) {
                    int futureID = futureArray[j].futureID;
                    assert(taskIDtoValue.containsKey(futureID));
                    stringArray[j] = taskIDtoValue.get(futureID);
                }
                input[i] = stringArray;
            }
        }
    }
}
