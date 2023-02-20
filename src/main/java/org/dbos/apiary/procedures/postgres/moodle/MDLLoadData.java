package org.dbos.apiary.procedures.postgres.moodle;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MDLLoadData extends PostgresFunction {
    // Load initial subscriptions data.
    private static final String subscribe =
            String.format("INSERT INTO %s(%s, %s) VALUES (?, ?);", MDLUtil.MDL_FORUMSUBS_TABLE, MDLUtil.MDL_USERID, MDLUtil.MDL_FORUMID);

    public static int runFunction(PostgresContext ctxt,
                                  int[] userIds, int[] forumIds) throws SQLException {
        List<Object[]> inputs = new ArrayList<>();
        for (int i = 0; i < userIds.length; i++) {
            Object[] input = new Object[2];
            input[0] = (long) userIds[i];
            input[1] = (long) forumIds[i];
            inputs.add(input);
        }
        ctxt.insertMany(subscribe, inputs);
        return userIds.length;
    }

    @Override
    public boolean isReadOnly() { return false; }

    @Override
    public List<String> writeTables() {
        return List.of(MDLUtil.MDL_FORUMSUBS_TABLE);
    }

}
