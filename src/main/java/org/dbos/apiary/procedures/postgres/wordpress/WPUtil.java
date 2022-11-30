package org.dbos.apiary.procedures.postgres.wordpress;

public class WPUtil {
    // For the posts table.
    public static final String WP_POSTS_TABLE = "WP_POSTS";
    public static final String WP_POST_ID = "POST_ID";
    public static final String WP_POST_CONTENT = "POST_CONTENT";
    public static final String WP_POST_STATUS = "POST_STATUS";
    public static final String WP_POSTS_SCHEMA = WP_POST_ID + " BIGINT PRIMARY KEY NOT NULL, "
            + WP_POST_CONTENT + " VARCHAR(2000) NOT NULL, "
            + WP_POST_STATUS + " VARCHAR(20) NOT NULL";

    // For the post metadata table.
    public static final String WP_POSTMETA_TABLE = "WP_POSTMETA";
    public static final String WP_META_KEY = "META_KEY";
    public static final String WP_META_VALUE = "META_VALUE";
    public static final String WP_TRASH_KEY = "_wp_trash_meta_comments_";
    public static final String WP_POSTMETA_SCHEMA = WP_POST_ID + " BIGINT NOT NULL, "
            + WP_META_KEY + " VARCHAR(255) NOT NULL, "
            + WP_META_VALUE + " VARCHAR(1000) NOT NULL";

    // For the comments table.
    public static final String WP_COMMENTS_TABLE = "WP_COMMENTS";
    public static final String WP_COMMENT_ID = "COMMENT_ID";
    public static final String WP_COMMENT_CONTENT = "COMMENT_CONTENT";
    public static final String WP_COMMENT_STATUS = "COMMENT_STATUS";
    public static final String WP_COMMENTS_SCHEMA = WP_COMMENT_ID + " BIGINT PRIMARY KEY NOT NULL, "
            + WP_POST_ID + " BIGINT NOT NULL, "
            + WP_COMMENT_CONTENT + " VARCHAR(2000) NOT NULL, "
            + WP_COMMENT_STATUS + " VARCHAR(20) NOT NULL";

    // For status.
    public static final String WP_STATUS_VISIBLE = "visible";
    public static final String WP_STATUS_TRASHED = "trashed";
    public static final String WP_STATUS_POST_TRASHED = "post-trashed";
}
