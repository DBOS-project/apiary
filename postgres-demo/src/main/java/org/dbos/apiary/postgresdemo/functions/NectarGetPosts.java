package org.dbos.apiary.postgresdemo.functions;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class NectarGetPosts extends PostgresFunction {

    private static final String getPosts = "SELECT Sender, PostText FROM WebsitePosts WHERE receiver=?";

    public static String[] runFunction(ApiaryStatefulFunctionContext ctxt, String username) throws SQLException {
        ResultSet rs = (ResultSet) ctxt.apiaryExecuteQuery(getPosts, username);
        List<String> results = new ArrayList<>();
        while (rs.next()) {
            results.add(rs.getString(1));
            results.add(rs.getString(2));
        }
        return results.toArray(new String[0]);
    }
}
