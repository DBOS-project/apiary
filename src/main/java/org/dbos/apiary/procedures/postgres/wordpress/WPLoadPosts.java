package org.dbos.apiary.procedures.postgres.wordpress;

import org.apache.commons.lang.RandomStringUtils;
import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class WPLoadPosts extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(WPLoadPosts.class);

    // ID, CONTENT, STATUS
    private static final String addPost = "INSERT INTO " + WPUtil.WP_POSTS_TABLE + " VALUES (?, ?, ?)";

    // CommentID, PostID, Comment, Status.
    private static final String addComment = "INSERT INTO " + WPUtil.WP_COMMENTS_TABLE + " VALUES(?, ?, ?, ?)";

    public static int runFunction(PostgresContext ctxt, int numPosts, int commentsPerPost) throws SQLException {

        List<Object[]> postInputs = new ArrayList<>();
        int cidIndex = 0;
        for (int i = 0; i < numPosts; i++) {
            long postId = i;
            Object[] input = new Object[3];
            input[0] = postId;
            input[1] = String.format("Post %d: This is a very very long post! %s ", postId, RandomStringUtils.randomAlphabetic(1000));
            input[2] = WPUtil.WP_STATUS_VISIBLE;
            postInputs.add(input);
        }
        ctxt.insertMany(addPost, postInputs);
        logger.info("Loaded {} posts.", numPosts);

        for (int i = 0; i < numPosts; i++) {
            long postId = i;
            // Add comments.
            List<Object[]> commentInputs = new ArrayList<>();
            for (int j = 0; j < commentsPerPost; j++) {
                long commentId = cidIndex;
                Object[] commentInput = new Object[4];
                commentInput[0] = commentId;
                commentInput[1] = postId;
                commentInput[2] = String.format("Comment %d for post %d: This is a very very long comment! %s", commentId, postId, RandomStringUtils.randomAlphabetic(1000));
                commentInput[3] = WPUtil.WP_STATUS_VISIBLE;
                commentInputs.add(commentInput);
                cidIndex++;
            }
            ctxt.insertMany(addComment, commentInputs);
            logger.debug("Loaded {} comments for postId {}", commentInputs.size(), postId);
        }
        if (numPosts * commentsPerPost != cidIndex) {
            logger.error("Mismatched expected length {} and actual length {}", numPosts * commentsPerPost, cidIndex);
            return -1;
        }
        logger.info("Loaded {} comments.", cidIndex);
        return cidIndex;
    }

    @Override
    public List<String> writeTables() {
        return List.of(WPUtil.WP_COMMENTS_TABLE, WPUtil.WP_POSTS_TABLE);
    }

}
