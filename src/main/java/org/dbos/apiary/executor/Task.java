package org.dbos.apiary.executor;

import org.dbos.apiary.interposition.ApiaryFuture;

import java.io.Serializable;
import java.util.Map;

public class Task implements Serializable  {

    public long taskID;  // Unique ID of this task.
    public final String funcName;
    public final Object[] input;

    // Initialize from user input.
    public Task(long taskID, String funcName, Object[] input) {
        this.taskID = taskID;
        this.funcName = funcName;
        this.input = input;
    }

    // Fill out the actual value of the referred future ID.
    // Return false if failed to resolve.
    public boolean dereferenceFutures(Map<Long, Object> taskIDtoValue) {
        boolean allResolved = true;
        for (int i = 0; i < input.length; i++) {
            Object o = input[i];
            if (o instanceof ApiaryFuture) {
                long futureID = ((ApiaryFuture) o).futureID;
                if (!taskIDtoValue.containsKey(futureID)) {
                    allResolved = false;
                } else {
                    input[i] = taskIDtoValue.get(futureID);
                }
            } else if (o instanceof ApiaryFuture[]) {
                ApiaryFuture[] futureArray = (ApiaryFuture[]) o;
                for (ApiaryFuture apiaryFuture : futureArray) {
                    long futureID = apiaryFuture.futureID;
                    if (!taskIDtoValue.containsKey(futureID)) {
                        allResolved = false;
                        break;
                    }
                }
                if (!allResolved) {
                    // Skip populating this input.
                    continue;
                }
                Object typeObject = taskIDtoValue.get(futureArray[0].futureID);
                if (typeObject instanceof String) {
                    String[] array = new String[futureArray.length];
                    for (int j = 0; j < futureArray.length; j++) {
                        long futureID = futureArray[j].futureID;
                        array[j] = (String) taskIDtoValue.get(futureID);
                        input[i] = array;
                    }
                } else if (typeObject instanceof Integer) {
                    int[] array = new int[futureArray.length];
                    for (int j = 0; j < futureArray.length; j++) {
                        long futureID = futureArray[j].futureID;
                        array[j] = (int) taskIDtoValue.get(futureID);
                        input[i] = array;
                    }
                } else if (typeObject instanceof String[]) {
                    String[][] array = new String[futureArray.length][];
                    for (int j = 0; j < futureArray.length; j++) {
                        long futureID = futureArray[j].futureID;
                        array[j] = (String[]) taskIDtoValue.get(futureID);
                        input[i] = array;
                    }
                } else if (typeObject instanceof int[]) {
                    int[][] array = new int[futureArray.length][];
                    for (int j = 0; j < futureArray.length; j++) {
                        long futureID = futureArray[j].futureID;
                        array[j] = (int[]) taskIDtoValue.get(futureID);
                        input[i] = array;
                    }
                }
            }
        }
        return allResolved;
    }
}
