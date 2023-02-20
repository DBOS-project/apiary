package org.dbos.apiary.procedures.postgres.wordpress;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class WPGetPostComments extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(WPGetPostComments.class);

    private static final String getPost = String.format("SELECT %s, %s FROM %s WHERE %s = ?",
            WPUtil.WP_POST_STATUS, WPUtil.WP_POST_CONTENT, WPUtil.WP_POSTS_TABLE, WPUtil.WP_POST_ID);
    private static final String getComments = String.format("SELECT %s FROM %s WHERE %s = ? AND %s = ? ORDER BY %s LIMIT 50",
            WPUtil.WP_COMMENT_CONTENT, WPUtil.WP_COMMENTS_TABLE, WPUtil.WP_POST_ID, WPUtil.WP_COMMENT_STATUS, WPUtil.WP_COMMENT_ID);

    public static String[] runFunction(PostgresContext ctxt, int postId) throws SQLException {
        List<String> resList = new ArrayList<>();
        ResultSet r = ctxt.executeQuery(getPost, postId);
        if (!r.next()) {
            logger.debug("Post {} does not exist!", postId);
            return resList.toArray(new String[0]);
        }
        String status = r.getString(WPUtil.WP_POST_STATUS);
        if (status.equals(WPUtil.WP_STATUS_TRASHED)) {
            logger.debug("Post {} is trashed, not visible!", postId);
            return resList.toArray(new String[0]);
        }
        String content = r.getString(WPUtil.WP_POST_CONTENT);
        resList.add(content);

        // Get all comments.
        r = ctxt.executeQuery(getComments, postId, WPUtil.WP_STATUS_VISIBLE);
        while (r.next()) {
            resList.add(r.getString(WPUtil.WP_COMMENT_CONTENT));
        }

        return resList.toArray(new String[0]);
    }

    @Override
    public boolean isReadOnly() { return true; }

    @Override
    public List<String> readTables() {
        return List.of(WPUtil.WP_POSTS_TABLE, WPUtil.WP_COMMENTS_TABLE);
    }
}
