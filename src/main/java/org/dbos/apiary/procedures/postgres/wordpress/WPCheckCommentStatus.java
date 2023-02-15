package org.dbos.apiary.procedures.postgres.wordpress;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class WPCheckCommentStatus extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(WPCheckCommentStatus.class);

    // Return a list of statuses of all comments of a post.
    private static final String checkStatus = String.format("SELECT %s, COUNT(*) FROM %s WHERE %s = ? GROUP BY %s",
            WPUtil.WP_COMMENT_STATUS, WPUtil.WP_COMMENTS_TABLE, WPUtil.WP_POST_ID, WPUtil.WP_COMMENT_STATUS);

    public static String[] runFunction(PostgresContext ctxt, int postId) throws SQLException {
        List<String> statusList = new ArrayList<>();
        ResultSet r = ctxt.executeQuery(checkStatus, postId);
        while (r.next()) {
            String status = r.getString(WPUtil.WP_COMMENT_STATUS);
            long cnt = r.getLong(2);
            logger.debug("Post {}, comment status {}, count {}", postId, status, cnt);
            statusList.add(status);
        }
        return statusList.toArray(new String[0]);
    }

    @Override
    public boolean isReadOnly() { return true; }

    @Override
    public List<String> readTables() {
        return List.of(WPUtil.WP_COMMENTS_TABLE);
    }
}
