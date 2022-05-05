package org.dbos.apiary.postgresdemo.functions;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class NectarAddPost extends PostgresFunction {

    private static final String addPost = "INSERT INTO WebsitePosts(Sender, Receiver, PostText) VALUES (?, ?, ?);";

    public static int runFunction(ApiaryStatefulFunctionContext ctxt, String sender, String receiver, String postText) {
        ctxt.apiaryExecuteUpdate(addPost, sender, receiver, postText);
        return 0;
    }
}
