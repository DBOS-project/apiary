package org.dbos.apiary.procedures.postgres.replay;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;

// Subscribe a user to a forum.
public class PostgresForumSubscribe extends PostgresFunction {
    private static final String subscribe = "INSERT INTO ForumSubscription(UserId, ForumId) VALUES (?, ?);";

    public static int runFunction(PostgresContext ctxt, int userId, int forumId) throws SQLException {
        ctxt.executeUpdate(subscribe, userId, forumId);
        return userId;
    }
}
