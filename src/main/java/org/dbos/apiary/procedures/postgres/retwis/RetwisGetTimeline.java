package org.dbos.apiary.procedures.postgres.retwis;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;

public class RetwisGetTimeline extends PostgresFunction {

    public static String runFunction(ApiaryStatefulFunctionContext ctxt, int userID) throws SQLException {
        String followees = ctxt.apiaryCallFunction(ctxt, "org.dbos.apiary.procedures.postgres.retwis.RetwisGetFollowees", userID).getString();
        String[] followeesList = followees.split(",");
        String sep = "";
        StringBuilder posts = new StringBuilder();
        for (String followee: followeesList) {
            String userPosts = ctxt.apiaryCallFunction(ctxt, "org.dbos.apiary.procedures.postgres.retwis.RetwisGetPosts", Integer.valueOf(followee)).getString();
            posts.append(sep);
            posts.append(userPosts);
            sep = ",";
        }
        return posts.toString();
    }
}
