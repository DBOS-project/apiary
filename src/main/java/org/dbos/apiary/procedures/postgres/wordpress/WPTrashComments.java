package org.dbos.apiary.procedures.postgres.wordpress;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;
import java.util.List;

public class WPTrashComments extends PostgresFunction {
    private static final String trashComments = String.format("UPDATE %s SET %s = ? WHERE %s = ? AND %s = ?", WPUtil.WP_COMMENTS_TABLE, WPUtil.WP_COMMENT_STATUS, WPUtil.WP_POST_ID, WPUtil.WP_COMMENT_STATUS);

    // Return postId on success.
    public static int runFunction(PostgresContext ctxt, int postId) throws SQLException {
        // Trash all visible comments.
        ctxt.executeUpdate(trashComments, WPUtil.WP_STATUS_POST_TRASHED, postId, WPUtil.WP_STATUS_VISIBLE);
        return postId;
    }

    @Override
    public boolean isReadOnly() { return false; }

    @Override
    public List<String> writeTables() {
        return List.of(WPUtil.WP_COMMENTS_TABLE);
    }
}
