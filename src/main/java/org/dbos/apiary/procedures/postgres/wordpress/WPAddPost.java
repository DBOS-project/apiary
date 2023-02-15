package org.dbos.apiary.procedures.postgres.wordpress;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;
import java.util.List;

public class WPAddPost extends PostgresFunction {

    // ID, CONTENT, STATUS
    private static final String addPost = "INSERT INTO " + WPUtil.WP_POSTS_TABLE + " VALUES (?, ?, ?)";

    // Return 0 on success, -1 on failure.
    public static int runFunction(PostgresContext ctxt, int postId, String content) throws SQLException {
        ctxt.executeUpdate(addPost, postId, content, WPUtil.WP_STATUS_VISIBLE);
        return 0;
    }

    @Override
    public boolean isReadOnly() { return false; }

    @Override
    public List<String> writeTables() {
        return List.of(WPUtil.WP_POSTS_TABLE);
    }
}
