package org.dbos.apiary.procedures.postgres.retwis;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;

public class RetwisGetTimeline extends PostgresFunction {

    public static String runFunction(ApiaryStatefulFunctionContext ctxt, int userID) throws SQLException {
        int[] followees = ctxt.apiaryCallFunction(ctxt, "org.dbos.apiary.procedures.postgres.retwis.RetwisGetFollowees", userID).getIntArray();
        String sep = "";
        StringBuilder posts = new StringBuilder();
        for (int followee: followees) {
            String userPosts = ctxt.apiaryCallFunction(ctxt, "org.dbos.apiary.procedures.postgres.retwis.RetwisGetPosts", followee).getString();
            posts.append(sep);
            posts.append(userPosts);
            sep = ",";
        }
        return posts.toString();
    }
}
