package org.dbos.apiary.postgresdemo.functions;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;

public class NectarAddPost extends PostgresFunction {

    private static final String addPost = "INSERT INTO WebsitePosts(Sender, Receiver, PostText) VALUES (?, ?, ?);";

    public static int runFunction(PostgresContext ctxt, String sender, String receiver, String postText) throws SQLException {
        ctxt.executeUpdate(addPost, sender, receiver, postText);
        return 0;
    }
}
