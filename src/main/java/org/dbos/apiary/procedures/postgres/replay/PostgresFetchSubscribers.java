package org.dbos.apiary.procedures.postgres.replay;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PostgresFetchSubscribers extends PostgresFunction {

    private static final String getSubscribers =
            "SELECT UserId FROM ForumSubscription WHERE ForumId=?";

    public static int[] runFunction(PostgresContext ctxt,
                                    int forumId) throws SQLException {
        // Return a list of subscribers of a forum.
        ResultSet r = ctxt.executeQuery(getSubscribers, forumId);
        List<Integer> subscribers = new ArrayList<>();
        while (r.next()) {
            subscribers.add(r.getInt(1));
        }
        return subscribers.stream().mapToInt(i -> i).toArray();
    }
}
