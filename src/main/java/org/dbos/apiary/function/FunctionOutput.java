package org.dbos.apiary.function;

import java.util.List;

/**
 * FunctionOutput stores the output of a function. It supports all Apiary function return types.
 */
public class FunctionOutput {
    public final Object output;
    public final List<Task> queuedTasks;
    private List<String> writtenKeys;

    public FunctionOutput(Object output, List<Task> queuedTasks) {
        assert(output != null);
        this.output = output;
        this.queuedTasks = queuedTasks;
    }

    /**
     * Return a <code>String</code> output. If the output is not <code>String</code>, return <code>null</code>.
     * @return the <code>String</code> output.
     */
    public String getString() {
        return output instanceof String ? (String) output : null;
    }

    /**
     * Return an <code>Integer</code> output. If the output is not <code>Integer</code>, return <code>null</code>.
     * @return the <code>Integer</code> output.
     */
    public Integer getInt() {
        return output instanceof Integer ? (Integer) output : null;
    }

    /**
     * Return a <code>String</code> array output. If the output is not <code>String</code> array, return <code>null</code>.
     * @return the <code>String[]</code> output.
     */
    public String[] getStringArray() { return output instanceof String[] ? (String[]) output : null; }

    /**
     * Return an <code>Integer</code> array output. If the output is not <code>Integer</code> array, return <code>null</code>.
     * @return the <code>Integer[]</code> output.
     */
    public int[] getIntArray() { return output instanceof int[] ? (int[]) output : null; }

    /**
     * Return an {@link ApiaryFuture} output. If the output is not <code>ApiaryFuture</code>, return <code>null</code>.
     * @return the {@link ApiaryFuture} output.
     */
    public ApiaryFuture getFuture() {
        return output instanceof ApiaryFuture ? (ApiaryFuture) output : null;
    }


    /** Internal API **/

    public List<String> getWrittenKeys() {
        return writtenKeys;
    }

    public void setWrittenKeys(List<String> writtenKeys) {
        this.writtenKeys = writtenKeys;
    }


}
