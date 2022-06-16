package org.dbos.apiary.procedures.postgres.retwis;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RetwisGetTimeline extends PostgresFunction {

    public static String[] runFunction(PostgresContext ctxt, int userID) throws Exception {
        int[] followees = ctxt.apiaryCallFunction("RetwisGetFollowees", userID).getIntArray();
        List<String> posts = new ArrayList<>();
        for (int followee: followees) {
            String[] userPosts = ctxt.apiaryCallFunction("RetwisGetPosts", followee).getStringArray();
            posts.addAll(Arrays.asList(userPosts));
        }
        return posts.toArray(new String[0]);
    }
}
