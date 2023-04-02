package org.dbos.apiary.procedures.postgres.wordpress;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class WPOptionExists extends PostgresFunction {
    private static final String getOptionValue = String.format("SELECT %s FROM %s WHERE %s = ?",
            WPUtil.WP_OPTION_VALUE, WPUtil.WP_OPTIONS_TABLE, WPUtil.WP_OPTION_NAME);

    public static Object runFunction(PostgresContext ctxt, String optionName, String optionValue, String isAutoLoad) throws SQLException {
        ResultSet rs = ctxt.executeQuery(getOptionValue, optionName);
        if (rs.next()) {
            // If an option exists, then update the value.
            return ctxt.apiaryQueueFunction(WPUtil.FUNC_UPDATEOPTION, optionName, optionValue, isAutoLoad);
        }

        // Otherwise, call the InsertOption function.
        return ctxt.apiaryQueueFunction(WPUtil.FUNC_INSERTOPTION, optionName, optionValue, isAutoLoad);
    }

    @Override
    public boolean isReadOnly() { return true; }

    @Override
    public List<String> readTables() {
        return List.of(WPUtil.WP_OPTIONS_TABLE);
    }
}
