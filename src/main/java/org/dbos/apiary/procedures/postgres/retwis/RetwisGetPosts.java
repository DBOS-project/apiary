package org.dbos.apiary.procedures.postgres.retwis;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RetwisGetPosts extends PostgresFunction {
    private static final String getPosts = "SELECT Post FROM RetwisPosts WHERE UserID=? ORDER BY Timestamp LIMIT 10;";

    public static String runFunction(ApiaryStatefulFunctionContext ctxt, int userID) throws SQLException {
        ResultSet result = (ResultSet) ctxt.apiaryExecuteQuery(getPosts, userID);
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
