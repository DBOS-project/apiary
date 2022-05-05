package org.dbos.apiary.postgresdemo.functions;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class NectarRegister extends PostgresFunction {

    private static final String checkExists = "SELECT * FROM WebsiteLogins WHERE Username=?";
    private static final String register = "INSERT INTO WebsiteLogins(Username, Password) VALUES (?, ?);";

    public static int runFunction(ApiaryStatefulFunctionContext ctxt, String username, String password) throws SQLException {
        ResultSet exists = (ResultSet) ctxt.apiaryExecuteQuery(checkExists, username);
        if (exists.next()) {
            return 1;  // Failed registration, username already exists.
        }
        ctxt.apiaryExecuteUpdate(register, username, password);
        return 0;
    }
}
