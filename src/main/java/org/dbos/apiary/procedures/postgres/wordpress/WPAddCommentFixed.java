package org.dbos.apiary.procedures.postgres.wordpress;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class WPAddCommentFixed extends PostgresFunction {
    // The bug fix version of WP AddComment.
    private static final String checkPost = String.format("SELECT %s, %s FROM %s WHERE %s = ?",
            WPUtil.WP_POST_STATUS, WPUtil.WP_POST_ID, WPUtil.WP_POSTS_TABLE, WPUtil.WP_POST_ID);

    // Check if the post is being trashed.
    private static final String checkPostmeta = String.format("SELECT %s FROM %s WHERE %s = ? AND %s = ?;", WPUtil.WP_POST_ID, WPUtil.WP_POSTMETA_TABLE, WPUtil.WP_META_KEY, WPUtil.WP_POST_ID);

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

        // Check if the post is being trashed or already trashed. If so, do not add this comment.
        r = ctxt.executeQuery(checkPostmeta, WPUtil.WP_TRASH_KEY, postId);
        if (r.next()) {
            // The post is being trashed or already trashed. Do not add a comment to this post.
            return -1;
        }

        // Otherwise, add the comment to the post.
        ctxt.executeUpdate(addComment, commentId, postId, content, WPUtil.WP_STATUS_VISIBLE);
        return 0;
    }

    @Override
    public boolean isReadOnly() { return false; }

    @Override
    public List<String> readTables() {
        return List.of(WPUtil.WP_POSTS_TABLE);
    }

    @Override
    public List<String> writeTables() {
        return List.of(WPUtil.WP_COMMENTS_TABLE);
    }
}
