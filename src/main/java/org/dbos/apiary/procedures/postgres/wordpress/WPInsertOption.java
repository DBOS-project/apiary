package org.dbos.apiary.procedures.postgres.wordpress;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class WPInsertOption extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(WPInsertOption.class);

    private static final String insertOption = String.format("INSERT INTO %s(%s, %s, %s) VALUES (?, ?, ?); ", WPUtil.WP_OPTIONS_TABLE, WPUtil.WP_OPTION_NAME, WPUtil.WP_OPTION_VALUE, WPUtil.WP_AUTOLOAD);

    // Return 0 on success, -1 on failed because of error.
    public static int runFunction(PostgresContext ctxt, String optionName, String optionValue, String isAutoLoad) {
        try {
            ctxt.executeUpdate(insertOption, optionName, optionValue, isAutoLoad);
        } catch (SQLException e) {
            logger.error("Failed to insert: {}", e.getMessage());
            return -1;
        }
        return 0;
    }
}
