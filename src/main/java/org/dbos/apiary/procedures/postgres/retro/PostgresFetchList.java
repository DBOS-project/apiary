package org.dbos.apiary.procedures.postgres.retro;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PostgresFetchList extends PostgresFunction {
    // Fetch a list of subscribers.

    private static final String getSubscribers = "SELECT UserId FROM ForumSubscription WHERE ForumId=?";

    public static Object runFunction(PostgresContext ctxt, int forumId) throws SQLException {
        // Check subscribers of a forum.
        ResultSet r = ctxt.executeQuery(getSubscribers, forumId);
        List<Integer> subscribers = new ArrayList<>();
        while (r.next()) {
            subscribers.add(r.getInt(1));
        }
        int[] list = subscribers.stream().mapToInt(i -> i).toArray();

        return ctxt.apiaryQueueFunction("PostgresFetchCount", list);
    }
}
