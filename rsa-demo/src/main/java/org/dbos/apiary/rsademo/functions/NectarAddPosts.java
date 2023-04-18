package org.dbos.apiary.rsademo.functions;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;

public class NectarAddPosts extends PostgresFunction {

    private static final String addPost = "INSERT INTO WebsitePosts(Sender, Receiver, PostText) VALUES (?, ?, ?);";

    public static int runFunction(PostgresContext ctxt, String[] senders, String[] receivers, String[] postTexts) throws SQLException {
        for (int i = 0; i < senders.length; i++) {
            ctxt.executeUpdate(addPost, senders[i], receivers[i], postTexts[i]);
        }
        return 0;
    }
}
