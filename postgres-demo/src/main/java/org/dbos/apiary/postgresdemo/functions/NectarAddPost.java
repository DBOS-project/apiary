package org.dbos.apiary.postgresdemo.functions;

import org.dbos.apiary.function.ApiaryTransactionalContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class NectarAddPost extends PostgresFunction {

    private static final String addPost = "INSERT INTO WebsitePosts(Sender, Receiver, PostText) VALUES (?, ?, ?);";

    public static int runFunction(ApiaryTransactionalContext ctxt, String sender, String receiver, String postText) {
        ctxt.apiaryExecuteUpdate(addPost, sender, receiver, postText);
        return 0;
    }
}
