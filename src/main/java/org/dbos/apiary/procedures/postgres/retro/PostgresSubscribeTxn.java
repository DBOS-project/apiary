package org.dbos.apiary.procedures.postgres.retro;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

// Transactionally check if a user is subscribed to a forum. If not, subscribe them.
// This should be a bug fix for the non-atomic version.
public class PostgresSubscribeTxn extends PostgresFunction {
    private static final String isSubscribed =
            "SELECT UserId, ForumId FROM ForumSubscription WHERE UserId=? AND ForumId=?";

    private static final String subscribe =
            "INSERT INTO ForumSubscription(UserId, ForumId) VALUES (?, ?);";

    public static int runFunction(PostgresContext ctxt,
                                  int userId, int forumId) throws SQLException {
        // Check if the user has been subscribed to the forum before.
        ResultSet r = ctxt.executeQuery(isSubscribed, userId, forumId);

        if (r.next()) {
            // If a subscription exists, then directly return the userID
            // without subscribing a new one.
            return r.getInt(1);
        }

        // Otherwise, subscribe the user to the forum.
        ctxt.executeUpdate(subscribe, userId, forumId);
        return userId;
    }
}
