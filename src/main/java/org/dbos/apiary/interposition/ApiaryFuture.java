package org.dbos.apiary.interposition;

import java.io.Serializable;

public class ApiaryFuture implements Serializable {
    public final long futureID;

    public ApiaryFuture(long futureID) {
        this.futureID = futureID;
    }
}
