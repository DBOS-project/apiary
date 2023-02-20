package org.dbos.apiary.procedures.postgres.moodle;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.dbos.apiary.procedures.postgres.wordpress.WPUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MDLFetchSubscribers extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(MDLFetchSubscribers.class);

    private static final String getSubscribers =
            String.format("SELECT %s FROM %s WHERE %s=?", MDLUtil.MDL_USERID, MDLUtil.MDL_FORUMSUBS_TABLE, MDLUtil.MDL_FORUMID);

    public static int[] runFunction(PostgresContext ctxt,
                                    int forumId) throws SQLException {
        // Get a list of subscribers of a forum.
        ResultSet r = ctxt.executeQuery(getSubscribers, forumId);
        List<Integer> subscribers = new ArrayList<>();
        while (r.next()) {
            subscribers.add((int) r.getLong(1));
        }

        // Check for duplicates.
        int[] resList = subscribers.stream().mapToInt(i -> i).toArray();
        // logger.info("Forum {} has subscribers: {}", forumId, resList);
        Set<Integer> unique = new HashSet<>();
        for (int i : resList) {
            if (!unique.add(i)) {
                logger.debug("Duplicated subscriptions for forum {}, userId {}", forumId, i);
            }
        }

        return resList;
    }

    @Override
    public boolean isReadOnly() { return true; }

    @Override
    public List<String> readTables() {
        return List.of(MDLUtil.MDL_FORUMSUBS_TABLE);
    }
}
