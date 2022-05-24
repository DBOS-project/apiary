package org.dbos.apiary.procedures.postgres.retwis;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;

public class RetwisFollow extends PostgresFunction {
    private static final String addFollowee = "INSERT INTO RetwisFollowees(UserID, FolloweeID) VALUES (?, ?);";

    public static int runFunction(PostgresContext ctxt, int userID, int followeeID) throws SQLException {
        ctxt.executeUpdate(addFollowee, userID, followeeID);
        return userID;
    }
}
