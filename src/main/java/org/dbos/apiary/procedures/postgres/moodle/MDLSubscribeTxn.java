package org.dbos.apiary.procedures.postgres.moodle;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

// Check if a user is subscribed to a forum and subscribe in one transaction.
public class MDLSubscribeTxn extends PostgresFunction {
    private static final String isSubscribed =
            String.format("SELECT %s, %s FROM %s WHERE %s=? AND %s=?",
                    MDLUtil.MDL_USERID, MDLUtil.MDL_FORUMID, MDLUtil.MDL_FORUMSUBS_TABLE, MDLUtil.MDL_USERID, MDLUtil.MDL_FORUMID);

    private static final String subscribe =
            String.format("INSERT INTO %s(%s, %s) VALUES (?, ?);", MDLUtil.MDL_FORUMSUBS_TABLE, MDLUtil.MDL_USERID, MDLUtil.MDL_FORUMID);

    public static int runFunction(PostgresContext ctxt,
                                  int userId, int forumId) throws SQLException {
        // Check if the user has been subscribed to the forum before.
        ResultSet r = ctxt.executeQuery(isSubscribed, (long) userId, (long) forumId);

        if (r.next()) {
            // If a subscription exists, then directly return the userID
            // without subscribing a new one.
            return r.getInt(1);
        }

        // Otherwise, subscribe the user to the forum.
        ctxt.executeUpdate(subscribe, (long) userId, (long) forumId);
        return userId;
    }

    @Override
    public boolean isReadOnly() { return false; }

    @Override
    public List<String> readTables() {
        return List.of(MDLUtil.MDL_FORUMSUBS_TABLE);
    }

    @Override
    public List<String> writeTables() {
        return List.of(MDLUtil.MDL_FORUMSUBS_TABLE);
    }
}
