package org.dbos.apiary.procedures.postgres.retwis;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;

public class RetwisPost extends PostgresFunction {
    private static final String addPost = "INSERT INTO RetwisPosts(UserID, PostID, Timestamp, Post) VALUES (?, ?, ?, ?);";

    public static int runFunction(ApiaryStatefulFunctionContext ctxt, int userID, int postID, int timestamp, String post) throws SQLException {
        ctxt.apiaryExecuteUpdate(addPost, userID, postID, timestamp, post);
        return userID;
    }
}
