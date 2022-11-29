package org.dbos.apiary.procedures.postgres.wordpress;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;

public class WPAddPost extends PostgresFunction {

    // ID, CONTENT, STATUS
    private static final String addPost = "INSERT INTO " + WPUtil.WP_POSTS_TABLE + " VALUES (?, ?, ?)";

    // Return 0 on success, -1 on failure.
    public static int runFunction(PostgresContext ctxt, int postId, String content) throws SQLException {
        ctxt.executeUpdate(addPost, postId, content, WPUtil.WP_STATUS_VISIBLE);
        return 0;
    }
}
