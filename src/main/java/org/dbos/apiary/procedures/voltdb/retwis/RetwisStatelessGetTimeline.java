package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.interposition.StatelessFunction;

public class RetwisStatelessGetTimeline extends StatelessFunction {

    public String runFunction(String userIDString) {
        String followees = (String) getContext().apiaryCallFunction("RetwisGetFollowees", userIDString);
        String[] followeesList = followees.split(",");
        StringBuilder ret = new StringBuilder();
        String sep = "";
        for (String followee: followeesList) {
            ret.append(sep);
            String posts = (String) getContext().apiaryCallFunction("RetwisGetPosts", followee);
            ret.append(posts);
            sep = ",";
        }
        return ret.toString();
    }
}
