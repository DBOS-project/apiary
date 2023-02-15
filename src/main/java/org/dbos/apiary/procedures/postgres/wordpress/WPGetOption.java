package org.dbos.apiary.procedures.postgres.wordpress;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class WPGetOption extends PostgresFunction {

    private static final String getOptionValue = String.format("SELECT %s FROM %s WHERE %s = ?",
            WPUtil.WP_OPTION_VALUE, WPUtil.WP_OPTIONS_TABLE, WPUtil.WP_OPTION_NAME);

    public static String runFunction(PostgresContext ctxt, String optionName) throws SQLException {
        ResultSet rs = ctxt.executeQuery(getOptionValue, optionName);
        if (!rs.next()) {
            return "none";
        }
        String optionValue = rs.getString(WPUtil.WP_OPTION_VALUE);
        return optionValue;
    }

    @Override
    public boolean isReadOnly() { return true; }

    @Override
    public List<String> readTables() {
        return List.of(WPUtil.WP_OPTIONS_TABLE);
    }
}
