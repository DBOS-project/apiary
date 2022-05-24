package org.dbos.apiary.procedures.postgres.retwis;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class RetwisGetPosts extends PostgresFunction {
    private static final String getPosts = "SELECT Post FROM RetwisPosts WHERE UserID=? ORDER BY Timestamp LIMIT 10;";

    public static String[] runFunction(PostgresContext ctxt, int userID) throws SQLException {
        ResultSet result = (ResultSet) ctxt.executeQuery(getPosts, userID);
        List<String> posts = new ArrayList<>();
        while (result.next()) {
            posts.add(result.getString(1));
        }
        return posts.toArray(new String[0]);
    }
}
