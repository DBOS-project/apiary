package org.dbos.apiary.rsademo.functions;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.json.simple.JSONObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class NectarGetPosts extends PostgresFunction {

    private static final String getPosts = "SELECT Sender, Receiver, PostText FROM WebsitePosts WHERE Receiver=?";

    public static String[] runFunction(PostgresContext ctxt, String username) throws SQLException {
        ResultSet rs = ctxt.executeQuery(getPosts, username);
        List<String> results = new ArrayList<>();
        while (rs.next()) {
            JSONObject obj = new JSONObject();
            obj.put("Sender", rs.getString(1));
            obj.put("Receiver", rs.getString(2));
            obj.put("PostText", rs.getString(3));
            results.add(obj.toJSONString());
        }
        return results.toArray(new String[0]);
    }
}
