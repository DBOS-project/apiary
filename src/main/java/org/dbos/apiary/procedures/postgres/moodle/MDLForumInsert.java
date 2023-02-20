package org.dbos.apiary.procedures.postgres.moodle;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

// Subscribe a user to a forum.
public class MDLForumInsert extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(MDLForumInsert.class);

    private static final String subscribe =
            String.format("INSERT INTO %s(%s, %s) VALUES (?, ?);", MDLUtil.MDL_FORUMSUBS_TABLE, MDLUtil.MDL_USERID, MDLUtil.MDL_FORUMID);

    public static int runFunction(PostgresContext ctxt,
                                  int userId, int forumId) throws SQLException {
        ctxt.executeUpdate(subscribe, (long) userId, (long) forumId);
        //logger.info("Added a subscription for user {}, forum {}", userId, forumId);
        return userId;
    }

    @Override
    public boolean isReadOnly() { return false; }

    @Override
    public List<String> writeTables() {
        return List.of(MDLUtil.MDL_FORUMSUBS_TABLE);
    }
}
