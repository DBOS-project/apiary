package org.dbos.apiary.postgresdemo.functions;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class NectarLogin extends PostgresFunction {

    private static final String checkPassword = "SELECT Username, Password FROM WebsiteLogins WHERE Username=?";

    public static int runFunction(PostgresContext ctxt, String username, String password) throws SQLException {
        ResultSet pwdCheck = ctxt.executeQuery(checkPassword, username);
        if (pwdCheck.next() && pwdCheck.getString(2).equals(password)) {
            return 0; // Success!
        } else {
            return 1; // Failed login: the user does not exist or the password is wrong.
        }
    }
}
