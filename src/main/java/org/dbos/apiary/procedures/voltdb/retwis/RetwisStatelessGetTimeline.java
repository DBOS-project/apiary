package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.interposition.ApiaryStatelessFunctionContext;
import org.dbos.apiary.interposition.StatelessFunction;

public class RetwisStatelessGetTimeline extends StatelessFunction {

    public ApiaryFuture runFunction(ApiaryStatelessFunctionContext ctxt, String userIDString) {
        String followees = (String) ctxt.apiaryCallFunction(ctxt, "RetwisGetFollowees", userIDString);
        String[] followeesList = followees.split(",");
        ApiaryFuture[] futures = new ApiaryFuture[followeesList.length];
        for (int i = 0; i < followeesList.length; i++) {
            futures[i] = ctxt.apiaryQueueFunction("RetwisGetPosts", String.valueOf(followeesList[i]));
        }
        return ctxt.apiaryQueueFunction("RetwisMerge", (Object) futures);
    }
}
