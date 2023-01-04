package org.dbos.apiary.procedures.postgres.replay;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

// Check if a user is subscribed to a forum.
public class MDLIsSubscribed extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(MDLIsSubscribed.class);
    private static final String isSubscribed =
            "SELECT UserId, ForumId FROM ForumSubscription WHERE UserId=? AND ForumId=?";

    public static Object runFunction(PostgresContext ctxt,
                                     int userId, int forumId) throws SQLException {
        // Check if the user has been subscribed to the forum before.
        ResultSet r = ctxt.executeQuery(isSubscribed, userId, forumId);

        if (r.next()) {
            // If a subscription exists, then directly return the userID
            // without subscribing a new one.
            logger.info("User {} has subscribed to forum {}.", userId, forumId);
            return r.getInt(1);
        }
        logger.info("User {} has not subscribed to forum {}.", userId, forumId);

        // Otherwise, call the ForumSubscribe function.
        return ctxt.apiaryQueueFunction("MDLForumInsert", userId, forumId);
    }
}
