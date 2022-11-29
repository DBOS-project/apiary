package org.dbos.apiary.procedures.postgres.wordpress;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class WPAddComment extends PostgresFunction {

    private static final String checkPost = "SELECT " + WPUtil.WP_POST_ID + " FROM " + WPUtil.WP_POSTS_TABLE + " WHERE " + WPUtil.WP_POST_ID + " = ?;";

    // CommentID, PostID, Comment, Status.
    private static final String addComment = "INSERT INTO " + WPUtil.WP_COMMENTS_TABLE + " VALUES(?, ?, ?, ?)";

    // Return 0 on success, -1 on failure.
    public static int runFunction(PostgresContext ctxt, long postId, long commentId, String content) throws SQLException {
        // Check if the post exists.
        ResultSet r = ctxt.executeQuery(checkPost, postId);
        if (!r.next()) {
            // Does not exist.
            return -1;
        }
        // Otherwise, add a comment.
        ctxt.executeUpdate(addComment, commentId, postId, content, WPUtil.WP_STATUS_VISIBLE);
        return 0;
    }
}
