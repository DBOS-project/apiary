package org.dbos.apiary.procedures.postgres.wordpress;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class WPTrashPost extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(WPTrashPost.class);

    private static final String checkPost = String.format("SELECT %s, %s FROM %s WHERE %s = ?;", WPUtil.WP_POST_ID, WPUtil.WP_POST_STATUS, WPUtil.WP_POSTS_TABLE, WPUtil.WP_POST_ID);

    private static final String trashPost = String.format("UPDATE %s SET %s = ? WHERE %s = ?;", WPUtil.WP_POSTS_TABLE, WPUtil.WP_POST_STATUS, WPUtil.WP_POST_ID);

    private static final String getComments = String.format("SELECT %s FROM %s WHERE %s = ? AND %s = ?;",
            WPUtil.WP_COMMENT_ID, WPUtil.WP_COMMENTS_TABLE, WPUtil.WP_POST_ID, WPUtil.WP_COMMENT_STATUS);

    // PostID, metaKey, metaValue
    private static final String storeMeta = String.format("INSERT INTO %s VALUES (?, ?, ?);", WPUtil.WP_POSTMETA_TABLE);

    // Find a post, trash the post, and find all visible comments and record them in the postmeta table, and finally queue a function to trash comments.
    // Return -1 on error, return 0 on no queued function, return a queued function if it has comments.
    public static Object runFunction(PostgresContext ctxt, int postId) throws SQLException {
        ResultSet r = ctxt.executeQuery(checkPost, postId);
        if (!r.next()) {
            logger.error("Post {} does not exist. Cannot trash it.", postId);
            return -1;
        }

        String postStatus = r.getString(WPUtil.WP_POST_STATUS);
        if (postStatus.equals(WPUtil.WP_STATUS_TRASHED)) {
            logger.info("Post {} has been trashed. Skipped.", postId);
            return 0;
        }
        // Trash the post.
        ctxt.executeUpdate(trashPost, WPUtil.WP_STATUS_TRASHED, postId);

        // Get comment IDs.
        r = ctxt.executeQuery(getComments, postId, WPUtil.WP_STATUS_VISIBLE);
        List<String> commentIds = new ArrayList<>();
        while (r.next()) {
            commentIds.add(String.valueOf(r.getLong(WPUtil.WP_COMMENT_ID)));
        }
        if (commentIds.isEmpty()) {
            logger.info("No comments to this post {}. Skip trash comments.", postId);
        }

        // Store postmeta.
        String commentIdStr = String.join(",", commentIds);
        ctxt.executeUpdate(storeMeta, postId, WPUtil.WP_TRASH_KEY, commentIdStr);

        return ctxt.apiaryQueueFunction("WPTrashComments", postId);
    }

    @Override
    public boolean isReadOnly() { return false; }

    @Override
    public List<String> readTables() {
        return List.of(WPUtil.WP_POSTS_TABLE, WPUtil.WP_COMMENTS_TABLE);
    }

    @Override
    public List<String> writeTables() {
        return List.of(WPUtil.WP_POSTS_TABLE, WPUtil.WP_POSTMETA_TABLE);
    }
}
