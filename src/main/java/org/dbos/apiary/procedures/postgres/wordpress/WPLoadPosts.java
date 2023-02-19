package org.dbos.apiary.procedures.postgres.wordpress;

import org.apache.commons.lang.RandomStringUtils;
import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class WPLoadPosts extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(WPLoadPosts.class);

    // ID, CONTENT, STATUS
    private static final String addPost = "INSERT INTO " + WPUtil.WP_POSTS_TABLE + " VALUES (?, ?, ?)";

    // CommentID, PostID, Comment, Status.
    private static final String addComment = "INSERT INTO " + WPUtil.WP_COMMENTS_TABLE + " VALUES(?, ?, ?, ?)";

    public static int runFunction(PostgresContext ctxt, int[] postIds, int[] commentIds) throws SQLException {
        if ((commentIds.length % postIds.length) != 0) {
            logger.error("Mismatched post IDs length {} and comment IDs length {}", postIds.length, commentIds.length);
        }
        int commentsPerPost = commentIds.length / postIds.length;
        List<Object[]> postInputs = new ArrayList<>();
        int cidIndex = 0;
        for (int i = 0; i < postIds.length; i++) {
            long postId = postIds[i];
            Object[] input = new Object[3];
            input[0] = postId;
            input[1] = String.format("Post %d: This is a very very long post! %s ", postId, RandomStringUtils.randomAlphabetic(1000));
            input[2] = WPUtil.WP_STATUS_VISIBLE;
        }
        ctxt.insertMany(addPost, postInputs);

        for (int i = 0; i < postIds.length; i++) {
            long postId = postIds[i];
            // Add comments.
            List<Object[]> commenInputs = new ArrayList<>();
            for (int j = 0; j < commentsPerPost; j++) {
                long commentId = commentIds[cidIndex];
                Object[] commentInput = new Object[4];
                commentInput[0] = commentId;
                commentInput[1] = postId;
                commentInput[2] = String.format("Comment %d for post %d: This is a very very long comment! %s", commentId, postId, RandomStringUtils.randomAlphabetic(1000));
                cidIndex++;
            }
            ctxt.insertMany(addComment, commenInputs);
        }
        if (commentIds.length != cidIndex) {
            logger.error("Mismatched expected length {} and actual length {}", commentIds.length, cidIndex);
            return -1;
        }
        return 0;
    }

}
