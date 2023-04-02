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
    public static final String WP_POSTMETA_INDEX = String.format("CREATE INDEX WPMETAPOSTID ON %s (%s, %s);", WP_POSTMETA_TABLE, WP_POST_ID, WP_META_KEY);

    // For the comments table.
    public static final String WP_COMMENTS_TABLE = "WP_COMMENTS";
    public static final String WP_COMMENT_ID = "COMMENT_ID";
    public static final String WP_COMMENT_CONTENT = "COMMENT_CONTENT";
    public static final String WP_COMMENT_STATUS = "COMMENT_STATUS";
    public static final String WP_COMMENTS_SCHEMA = WP_COMMENT_ID + " BIGINT PRIMARY KEY NOT NULL, "
            + WP_POST_ID + " BIGINT NOT NULL, "
            + WP_COMMENT_CONTENT + " VARCHAR(2000) NOT NULL, "
            + WP_COMMENT_STATUS + " VARCHAR(20) NOT NULL";
    public static final String WP_COMMENTSPOSTID_INDEX = String.format("CREATE INDEX WPCOMMENTSPOSTID ON %s (%s);", WP_COMMENTS_TABLE, WP_POST_ID);

    // For status.
    public static final String WP_STATUS_VISIBLE = "visible";
    public static final String WP_STATUS_TRASHED = "trashed";
    public static final String WP_STATUS_POST_TRASHED = "post-trashed";

    // For the options table.
    public static final String WP_OPTIONS_TABLE = "WP_OPTIONS";
    public static final String WP_OPTION_NAME = "OPTION_NAME";
    public static final String WP_OPTION_VALUE = "OPTION_VALUE";
    public static final String WP_AUTOLOAD = "AUTOLOAD";
    public static final String WP_OPTIONS_SCHEMA = WP_OPTION_NAME + " VARCHAR(128) PRIMARY KEY NOT NULL, "
            + WP_OPTION_VALUE + " VARCHAR(2000) NOT NULL, "
            + WP_AUTOLOAD + " VARCHAR(20) NOT NULL DEFAULT 'yes' ";

    // For functions.
    public static final String FUNC_ADDPOST = "WPAddPost";
    public static final String FUNC_ADDCOMMENT = "WPAddComment";
    public static final String FUNC_GETPOSTCOMMENTS= "WPGetPostComments";
    public static final String FUNC_TRASHPOST = "WPTrashPost";
    public static final String FUNC_TRASHCOMMENTS = "WPTrashComments";
    public static final String FUNC_UNTRASHPOST = "WPUntrashPost";
    public static final String FUNC_COMMENTSTATUS = "WPCheckCommentStatus";
    public static final String FUNC_GETOPTION = "WPGetOption";
    public static final String FUNC_OPTIONEXISTS = "WPOptionExists";
    public static final String FUNC_INSERTOPTION = "WPInsertOption";
    public static final String FUNC_UPDATEOPTION = "WPUpdateOption";
    public static final String FUNC_LOAD_POSTS = "WPLoadPosts";
    public static final String FUNC_LOAD_OPTIONS = "WPLoadOptions";
}
