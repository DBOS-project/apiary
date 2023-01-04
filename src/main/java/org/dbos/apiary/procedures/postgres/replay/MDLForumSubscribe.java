package org.dbos.apiary.procedures.postgres.replay;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

// Subscribe a user to a forum.
public class MDLForumSubscribe extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(MDLForumSubscribe.class);

    private static final String subscribe =
            "INSERT INTO ForumSubscription(UserId, ForumId) VALUES (?, ?);";

    public static int runFunction(PostgresContext ctxt,
                                  int userId, int forumId) throws SQLException {
        ctxt.executeUpdate(subscribe, userId, forumId);
        logger.info("Added a subscription for user {}, forum {}", userId, forumId);
        return userId;
    }
}
