package org.dbos.apiary.procedures.postgres.crossdb;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgresSearchPerson extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(PostgresSearchPerson.class);

    private final static String search = "SELECT COUNT(*) FROM PersonTable WHERE Name=?";

    public int runFunction(PostgresContext context, String searchText) throws Exception {
        ResultSet rs = context.executeQuery(search, searchText);
        rs.next();
        int pgCount = rs.getInt(1);
        int esCount = context.apiaryCallFunction("ElasticsearchSearchPerson", searchText).getInt();
        if (pgCount == esCount) {
            return pgCount;
        } else {
            logger.info("{} {} {} {} {} {} {}", searchText, pgCount, esCount,
                    context.txc.txID, context.txc.xmin, context.txc.xmax, context.txc.activeTransactions);
            return -1;
        }
    }
}
