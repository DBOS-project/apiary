package org.dbos.apiary.procedures.postgres.pggcs;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;

public class PostgresProfileRead extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(PostgresProfileRead.class);

    private final static String search = "SELECT Status FROM ProfileTable WHERE UserID=?";

    public int runFunction(PostgresContext context, int userID) throws Exception {
        ResultSet rs = context.executeQuery(search, userID);
        if (!rs.next()) {
            return -1;
        }
        return context.apiaryCallFunction("GCSProfileRead", userID).getInt();
    }
}
