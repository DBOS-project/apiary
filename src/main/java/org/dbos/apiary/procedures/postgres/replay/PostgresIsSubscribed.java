package org.dbos.apiary.procedures.postgres.replay;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

// Check if a user is subscribed to a forum.
public class PostgresIsSubscribed extends PostgresFunction {
    private static final String isSubscribed =
            "SELECT UserId, ForumId FROM ForumSubscription WHERE UserId=? AND ForumId=?";

    public static Object runFunction(PostgresContext ctxt,
                                     int userId, int forumId) throws SQLException {
        // Check if the user has been subscribed to the forum before.
        ResultSet r = ctxt.executeQuery(isSubscribed, userId, forumId);

        if (r.next()) {
            // If a subscription exists, then directly return the userID
            // without subscribing a new one.
            return r.getInt(1);
        }

        // Otherwise, call the ForumSubscribe function.
        return ctxt.apiaryQueueFunction("PostgresForumSubscribe", userId, forumId);
    }
}
