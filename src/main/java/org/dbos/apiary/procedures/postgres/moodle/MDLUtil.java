package org.dbos.apiary.procedures.postgres.moodle;

public class MDLUtil {
    public static final String MDL_FORUMSUBS_TABLE = "ForumSubscription";
    public static final String MDL_FORUMID = "ForumId";
    public static final String MDL_USERID = "UserId";
    public static final String MDL_FORUM_SCHEMA = String.format("%s BIGINT NOT NULL, %s BIGINT NOT NULL", MDLUtil.MDL_USERID, MDLUtil.MDL_FORUMID);
    public static final String MDL_FORUMID_INDEX = String.format("CREATE INDEX FORUMID ON %s (%s);", MDL_FORUMSUBS_TABLE, MDL_FORUMID);

    // For functions.
    public static final String FUNC_FETCH_SUBSCRIBERS = "MDLFetchSubscribers";
    public static final String FUNC_FORUM_INSERT = "MDLForumInsert";
    public static final String FUNC_IS_SUBSCRIBED = "MDLIsSubscribed";
    public static final String FUNC_LOAD_DATA = "MDLLoadData";
}
