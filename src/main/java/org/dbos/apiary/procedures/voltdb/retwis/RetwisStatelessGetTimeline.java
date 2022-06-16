package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.ApiaryFuture;
import org.dbos.apiary.function.StatelessFunction;

public class RetwisStatelessGetTimeline extends StatelessFunction {

    public static ApiaryFuture runFunction(ApiaryContext ctxt, int userID) throws Exception {
        int[] followees = ctxt.apiaryCallFunction("RetwisGetFollowees", userID).getIntArray();
        ApiaryFuture[] futures = new ApiaryFuture[followees.length];
        for (int i = 0; i < followees.length; i++) {
            futures[i] = ctxt.apiaryQueueFunction("RetwisGetPosts", followees[i]);
        }
        return ctxt.apiaryQueueFunction("RetwisMerge", (Object) futures);
    }
}
