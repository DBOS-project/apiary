package org.dbos.apiary.procedures.postgres.wordpress;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;

// This function fixes WPInsertOption function, by using ON CONFLICT to avoid failures.
public class WPInsertOptionFixed extends PostgresFunction {
    /* This cannot fix the bug because concurrent transactions will throw "ERROR: could not serialize access due to concurrent update"
    private static final String insertOption = String.format("INSERT INTO %s(%s, %s, %s) VALUES (?, ?, ?) ON CONFLICT (%s) DO UPDATE SET %s = EXCLUDED.%s, %s = EXCLUDED.%s; ", WPUtil.WP_OPTIONS_TABLE, WPUtil.WP_OPTION_NAME, WPUtil.WP_OPTION_VALUE, WPUtil.WP_AUTOLOAD, WPUtil.WP_OPTION_NAME,

            WPUtil.WP_OPTION_VALUE, WPUtil.WP_OPTION_VALUE,
            WPUtil.WP_AUTOLOAD, WPUtil.WP_AUTOLOAD);
    */
    private static final String insertOption = String.format("INSERT INTO %s(%s, %s, %s) VALUES (?, ?, ?) ON CONFLICT (%s) DO NOTHING; ", WPUtil.WP_OPTIONS_TABLE, WPUtil.WP_OPTION_NAME, WPUtil.WP_OPTION_VALUE, WPUtil.WP_AUTOLOAD, WPUtil.WP_OPTION_NAME);

    // Return 0 on success.
    public static int runFunction(PostgresContext ctxt, String optionName, String optionValue, String isAutoLoad) throws SQLException {
        ctxt.executeUpdate(insertOption, optionName, optionValue, isAutoLoad);
        return 0;
    }
}
