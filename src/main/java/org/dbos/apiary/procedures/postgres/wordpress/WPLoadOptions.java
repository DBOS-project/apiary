package org.dbos.apiary.procedures.postgres.wordpress;

import org.apache.commons.lang.RandomStringUtils;
import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class WPLoadOptions extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(WPLoadOptions.class);

    private static final String insertOption = String.format("INSERT INTO %s(%s, %s, %s) VALUES (?, ?, ?); ", WPUtil.WP_OPTIONS_TABLE, WPUtil.WP_OPTION_NAME, WPUtil.WP_OPTION_VALUE, WPUtil.WP_AUTOLOAD);

    public static int runFunction(PostgresContext ctxt, int beginId, int endId) throws SQLException {
        List<Object[]> optionInputs = new ArrayList<>();
        int cnt = 0;
        for (int i = beginId; i < endId; i++) {
            Object[] input = new Object[3];
            input[0] = "option-" + i;
            input[1] = String.format("value-newInsert-%d-%s", i, RandomStringUtils.randomAlphabetic(100));
            input[2] = "no";
            optionInputs.add(input);
            cnt++;
        }
        ctxt.insertMany(insertOption, optionInputs);
        logger.info("Loaded {} options.", cnt);
        return cnt;
    }

    @Override
    public List<String> writeTables() {
        return List.of(WPUtil.WP_OPTIONS_TABLE);
    }
}
