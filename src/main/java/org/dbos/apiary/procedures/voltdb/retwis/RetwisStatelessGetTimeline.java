package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.interposition.StatelessFunction;

public class RetwisStatelessGetTimeline extends StatelessFunction {

    public ApiaryFuture runFunction(String userIDString) {
        String followees = (String) getContext().apiaryCallFunction("RetwisGetFollowees", userIDString);
        String[] followeesList = followees.split(",");
        ApiaryFuture[] futures = new ApiaryFuture[followeesList.length];
        for (int i = 0; i < followeesList.length; i++) {
            futures[i] = getContext().apiaryQueueFunction("RetwisGetPosts", String.valueOf(followeesList[i]));
        }
        return getContext().apiaryQueueFunction("RetwisMerge", (Object) futures);
    }
}
