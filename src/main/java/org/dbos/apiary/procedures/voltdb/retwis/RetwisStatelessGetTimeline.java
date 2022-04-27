package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.interposition.StatelessFunction;

public class RetwisStatelessGetTimeline extends StatelessFunction {

    public static ApiaryFuture runFunction(ApiaryFunctionContext ctxt, int userID) {
        String followees = (String) ctxt.apiaryCallFunction(ctxt, "RetwisGetFollowees", userID);
        String[] followeesList = followees.split(",");
        ApiaryFuture[] futures = new ApiaryFuture[followeesList.length];
        for (int i = 0; i < followeesList.length; i++) {
            futures[i] = ctxt.apiaryQueueFunction("RetwisGetPosts", Integer.valueOf(followeesList[i]));
        }
        return ctxt.apiaryQueueFunction("RetwisMerge", (Object) futures);
    }
}
