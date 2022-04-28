package org.dbos.apiary.procedures.postgres.retwis;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RetwisGetFollowees extends PostgresFunction {
    private static final String getFollowees = "SELECT FolloweeID FROM RetwisFollowees WHERE UserID=?;";

    public static String runFunction(ApiaryStatefulFunctionContext ctxt, int userID) throws SQLException {
        ResultSet result = (ResultSet) ctxt.apiaryExecuteQuery(getFollowees, userID);
        StringBuilder posts = new StringBuilder();
        String sep = "";
        while (result.next()) { // TODO: Use properly escaped JSON or something.
            posts.append(sep);
            posts.append(result.getString(1));
            sep = ",";
        }
        return posts.toString();
    }
}
