package org.dbos.apiary.procedures.postgres.retwis;

import org.dbos.apiary.function.ApiaryTransactionalContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class RetwisGetFollowees extends PostgresFunction {
    private static final String getFollowees = "SELECT FolloweeID FROM RetwisFollowees WHERE UserID=?;";

    public static int[] runFunction(ApiaryTransactionalContext ctxt, int userID) throws SQLException {
        ResultSet result = (ResultSet) ctxt.apiaryExecuteQuery(getFollowees, userID);
        List<Integer> followees = new ArrayList<>();
        while (result.next()) {
            followees.add(result.getInt(1));
        }
        return followees.stream().mapToInt(i -> i).toArray();
    }
}
