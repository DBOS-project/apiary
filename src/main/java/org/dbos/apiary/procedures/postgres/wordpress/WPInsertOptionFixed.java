package org.dbos.apiary.procedures.postgres.wordpress;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;
import java.util.List;

// This function fixes WPInsertOption function, by using ON CONFLICT to avoid failures.
public class WPInsertOptionFixed extends PostgresFunction {
    // This can still throw an error under repeatable read or serializable level: "ERROR: could not serialize access due to concurrent update"
    private static final String insertOption = String.format("INSERT INTO %s(%s, %s, %s) VALUES (?, ?, ?) ON CONFLICT (%s) DO UPDATE SET %s = EXCLUDED.%s, %s = EXCLUDED.%s; ", WPUtil.WP_OPTIONS_TABLE, WPUtil.WP_OPTION_NAME, WPUtil.WP_OPTION_VALUE, WPUtil.WP_AUTOLOAD, WPUtil.WP_OPTION_NAME,

            WPUtil.WP_OPTION_VALUE, WPUtil.WP_OPTION_VALUE,
            WPUtil.WP_AUTOLOAD, WPUtil.WP_AUTOLOAD);

    // Return 0 on success.
    public static int runFunction(PostgresContext ctxt, String optionName, String optionValue, String isAutoLoad) throws SQLException {
        ctxt.executeUpdate(insertOption, optionName, optionValue, isAutoLoad);
        return 0;
    }

    @Override
    public boolean isReadOnly() { return false; }

    @Override
    public List<String> writeTables() {
        return List.of(WPUtil.WP_OPTIONS_TABLE);
    }
}
