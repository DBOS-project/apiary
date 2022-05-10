package org.dbos.apiary.interposition;

import java.io.Serializable;

/**
 * ApiaryFuture is used for asynchronous invocations.
 * The asynchronously invoked function would return an ApiaryFuture to the caller.
 * Then the caller function can pass the ApiaryFuture object as the input to other asynchronous invocations,
 * but cannot dereference the future in place.
 */
public class ApiaryFuture implements Serializable {
    public final long futureID;

    public ApiaryFuture(long futureID) {
        this.futureID = futureID;
    }
}
