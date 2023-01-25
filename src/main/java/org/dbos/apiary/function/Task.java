package org.dbos.apiary.function;

import java.io.Serializable;
import java.util.Map;

/**
 * For internal use only.
 */
public class Task implements Serializable  {

    public final long execId;
    public long functionID;  // Unique ID of this function.
    public final String funcName;
    public final Object[] input;

    // Initialize from user input.
    public Task(long execId, long functionID, String funcName, Object[] input) {
        this.execId = execId;
        this.functionID = functionID;
        this.funcName = funcName;
        this.input = input;
    }

    // Fill out the actual value of the referred future ID.
    // Return false if failed to resolve.
    public boolean dereferenceFutures(Map<Long, Object> functionIDToValue) {
        boolean allResolved = true;
        for (int i = 0; i < input.length; i++) {
            Object o = input[i];
            if (o instanceof ApiaryFuture) {
                long futureID = ((ApiaryFuture) o).futureID;
                if (!functionIDToValue.containsKey(futureID)) {
                    allResolved = false;
                } else {
                    input[i] = functionIDToValue.get(futureID);
                }
            } else if (o instanceof ApiaryFuture[]) {
                ApiaryFuture[] futureArray = (ApiaryFuture[]) o;
                for (ApiaryFuture apiaryFuture : futureArray) {
                    long futureID = apiaryFuture.futureID;
                    if (!functionIDToValue.containsKey(futureID)) {
                        allResolved = false;
                        break;
                    }
                }
                if (!allResolved) {
                    // Skip populating this input.
                    continue;
                }
                Object typeObject = functionIDToValue.get(futureArray[0].futureID);
                if (typeObject instanceof String) {
                    String[] array = new String[futureArray.length];
                    for (int j = 0; j < futureArray.length; j++) {
                        long futureID = futureArray[j].futureID;
                        array[j] = (String) functionIDToValue.get(futureID);
                        input[i] = array;
                    }
                } else if (typeObject instanceof Integer) {
                    int[] array = new int[futureArray.length];
                    for (int j = 0; j < futureArray.length; j++) {
                        long futureID = futureArray[j].futureID;
                        array[j] = (int) functionIDToValue.get(futureID);
                        input[i] = array;
                    }
                } else if (typeObject instanceof String[]) {
                    String[][] array = new String[futureArray.length][];
                    for (int j = 0; j < futureArray.length; j++) {
                        long futureID = futureArray[j].futureID;
                        array[j] = (String[]) functionIDToValue.get(futureID);
                        input[i] = array;
                    }
                } else if (typeObject instanceof int[]) {
                    int[][] array = new int[futureArray.length][];
                    for (int j = 0; j < futureArray.length; j++) {
                        long futureID = futureArray[j].futureID;
                        array[j] = (int[]) functionIDToValue.get(futureID);
                        input[i] = array;
                    }
                }
            }
        }
        return allResolved;
    }
}
