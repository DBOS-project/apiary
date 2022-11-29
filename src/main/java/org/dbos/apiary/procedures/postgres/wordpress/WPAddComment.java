package org.dbos.apiary.procedures.postgres.wordpress;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class WPAddComment extends PostgresFunction {

    private static final String checkPost = String.format("SELECT %s, %s FROM %s WHERE %s = ?",
            WPUtil.WP_POST_STATUS, WPUtil.WP_POST_ID, WPUtil.WP_POSTS_TABLE, WPUtil.WP_POST_ID);

    // CommentID, PostID, Comment, Status.
    private static final String addComment = "INSERT INTO " + WPUtil.WP_COMMENTS_TABLE + " VALUES(?, ?, ?, ?)";

    // Return 0 on success, -1 on failure.
    public static int runFunction(PostgresContext ctxt, int postId, int commentId, String content) throws SQLException {
        // Check if the post exists.
        ResultSet r = ctxt.executeQuery(checkPost, postId);
        if (!r.next()) {
            // Does not exist.
            return -1;
        }
        String postStatus = r.getString(WPUtil.WP_POST_STATUS);
        if (postStatus.equals(WPUtil.WP_STATUS_VISIBLE)) {
            ctxt.executeUpdate(addComment, commentId, postId, content, WPUtil.WP_STATUS_VISIBLE);
        } else {
            ctxt.executeUpdate(addComment, commentId, postId, content, WPUtil.WP_STATUS_POST_TRASHED);
        }
        return 0;
    }
}
