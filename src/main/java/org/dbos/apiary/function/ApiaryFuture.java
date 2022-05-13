package org.dbos.apiary.function;

import java.io.Serializable;

/**
 * ApiaryFuture is returned by asynchronously invoked functions.
 * Futures can be passed as arguments to other functions or returned,
 * but cannot be dereferenced directly.
 */
public class ApiaryFuture implements Serializable {
    public final long futureID;

    public ApiaryFuture(long futureID) {
        this.futureID = futureID;
    }
}
