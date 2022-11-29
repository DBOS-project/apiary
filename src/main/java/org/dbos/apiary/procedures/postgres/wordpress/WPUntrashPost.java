package org.dbos.apiary.procedures.postgres.wordpress;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

public class WPUntrashPost extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(WPUntrashPost.class);
    private static final String getMeta = String.format("SELECT %s FROM %s WHERE %s = ? AND %s = ?", WPUtil.WP_META_VALUE, WPUtil.WP_POSTMETA_TABLE, WPUtil.WP_POST_ID, WPUtil.WP_META_KEY);

    private static final String untrashPost = String.format("UPDATE %s SET %s = ? WHERE %s = ?;", WPUtil.WP_POSTS_TABLE, WPUtil.WP_POST_STATUS, WPUtil.WP_POST_ID);

    // Need to fill in actual comment IDs.
    private static final String untrashComments = "UPDATE %s SET %s = ? WHERE %s IN (%s)";

    private static final String deleteMeta = String.format("DELETE FROM %s WHERE %s = ? AND %s = ?;", WPUtil.WP_POSTMETA_TABLE, WPUtil.WP_POST_ID, WPUtil.WP_META_KEY);

    // Return 0 on success, -1 on failure.
    public static int runFunction(PostgresContext ctxt, int postId) throws SQLException {
        ResultSet r = ctxt.executeQuery(getMeta, postId, WPUtil.WP_TRASH_KEY);
        if (!r.next()) {
            logger.error("Cannot find metadata for post {}", postId);
            return -1;
        }
        String commentIdStr = r.getString(WPUtil.WP_META_VALUE);

        // Untrash post.
        ctxt.executeUpdate(untrashPost, WPUtil.WP_STATUS_VISIBLE, postId);

        // Untrash all comments.
        String updateQuery = String.format(untrashComments, WPUtil.WP_COMMENTS_TABLE, WPUtil.WP_COMMENT_STATUS, WPUtil.WP_COMMENT_ID, commentIdStr);
        ctxt.executeUpdate(updateQuery, WPUtil.WP_STATUS_VISIBLE);

        // Clean up metadata.
        ctxt.executeUpdate(deleteMeta, postId, WPUtil.WP_TRASH_KEY);
        return 0;
    }

}
