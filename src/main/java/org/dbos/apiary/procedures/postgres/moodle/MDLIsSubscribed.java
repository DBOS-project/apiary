package org.dbos.apiary.procedures.postgres.moodle;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

// Check if a user is subscribed to a forum.
public class MDLIsSubscribed extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(MDLIsSubscribed.class);
    private static final String isSubscribed =
            String.format("SELECT %s, %s FROM %s WHERE %s=? AND %s=?",
                    MDLUtil.MDL_USERID, MDLUtil.MDL_FORUMID, MDLUtil.MDL_FORUMSUBS_TABLE, MDLUtil.MDL_USERID, MDLUtil.MDL_FORUMID);

    public static Object runFunction(PostgresContext ctxt,
                                     int userId, int forumId) throws SQLException {
        // Check if the user has been subscribed to the forum before.
        ResultSet r = ctxt.executeQuery(isSubscribed, (long) userId, (long) forumId);

        if (r.next()) {
            // If a subscription exists, then directly return the userID
            // without subscribing a new one.
            return r.getInt(1);
        }
        // logger.info("User {} hasn't subscribed to forum {}.", userId, forumId);

        // Otherwise, call the ForumSubscribe function.
        return ctxt.apiaryQueueFunction("MDLForumInsert", userId, forumId);
    }

    @Override
    public boolean isReadOnly() { return true; }

    @Override
    public List<String> readTables() {
        return List.of(MDLUtil.MDL_FORUMSUBS_TABLE);
    }
}
