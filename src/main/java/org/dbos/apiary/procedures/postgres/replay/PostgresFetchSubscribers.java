package org.dbos.apiary.procedures.postgres.replay;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PostgresFetchSubscribers extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(PostgresFetchSubscribers.class);

    private static final String getSubscribers =
            "SELECT UserId FROM ForumSubscription WHERE ForumId=?";

    public static int[] runFunction(PostgresContext ctxt,
                                    int forumId) throws SQLException {
        // Get a list of subscribers of a forum.
        ResultSet r = ctxt.executeQuery(getSubscribers, forumId);
        List<Integer> subscribers = new ArrayList<>();
        while (r.next()) {
            subscribers.add(r.getInt(1));
        }

        // Check for duplicates.
        int[] resList = subscribers.stream().mapToInt(i -> i).toArray();
        boolean noDup = Utilities.checkDuplicates(resList);
        if (!noDup) {
            logger.error("Found duplicates!");
        }
        return resList;
    }
}
