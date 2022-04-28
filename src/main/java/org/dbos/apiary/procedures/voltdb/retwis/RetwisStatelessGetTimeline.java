package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.interposition.StatelessFunction;

public class RetwisStatelessGetTimeline extends StatelessFunction {

    public static ApiaryFuture runFunction(ApiaryFunctionContext ctxt, int userID) {
        int[] followees = ctxt.apiaryCallFunction(ctxt, "RetwisGetFollowees", userID).getIntArray();
        ApiaryFuture[] futures = new ApiaryFuture[followees.length];
        for (int i = 0; i < followees.length; i++) {
            futures[i] = ctxt.apiaryQueueFunction("RetwisGetPosts", followees[i]);
        }
        return ctxt.apiaryQueueFunction("RetwisMerge", (Object) futures);
    }
}
