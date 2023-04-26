package org.dbos.apiary.rsademo.functions;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class NectarRegister extends PostgresFunction {

    private static final String checkExists = "SELECT * FROM WebsiteLogins WHERE Username=?";
    private static final String register = "INSERT INTO WebsiteLogins(Username, Password) VALUES (?, ?);";

    public static int runFunction(PostgresContext ctxt, String username, String password) throws SQLException {
        ResultSet exists = ctxt.executeQuery(checkExists, username);
        if (exists.next()) {
            return 1;  // Failed registration, username already exists.
        }
        ctxt.executeUpdate(register, username, password);
        return 0;
    }
}
