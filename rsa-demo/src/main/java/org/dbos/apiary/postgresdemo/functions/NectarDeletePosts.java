package org.dbos.apiary.postgresdemo.functions;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;

public class NectarDeletePosts extends PostgresFunction {

    private static final String removePosts = "DELETE FROM WebsitePosts WHERE Sender=?;";

    public static int runFunction(PostgresContext ctxt, String sender) throws SQLException {
        ctxt.executeUpdate(removePosts, sender);
        return 0;
    }
}
