package org.dbos.apiary.procedures.postgres.moodle;

public class MDLUtil {
    public static final String MDL_FORUMSUBS_TABLE = "ForumSubscription";
    public static final String MDL_FORUMID = "ForumId";
    public static final String MDL_USERID = "UserId";
    public static final String MDL_FORUM_SCHEMA = String.format("%s BIGINT NOT NULL, %s BIGINT NOT NULL", MDLUtil.MDL_USERID, MDLUtil.MDL_FORUMID);
}
