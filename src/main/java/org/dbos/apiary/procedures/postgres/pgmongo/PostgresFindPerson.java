package org.dbos.apiary.procedures.postgres.pgmongo;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;

public class PostgresFindPerson extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(PostgresFindPerson.class);

    private final static String find = "SELECT COUNT(*) FROM PersonTable WHERE Name=?";

    public static int runFunction(PostgresContext ctxt, String search) throws Exception {
        ResultSet rs = ctxt.executeQuery(find, search);
        rs.next();
        int pgCount = rs.getInt(1);
        int esCount = ctxt.apiaryCallFunction("MongoFindPerson", search).getInt();
        if (!ApiaryConfig.XDBTransactions || pgCount == esCount) {
            return pgCount;
        } else {
            logger.info("{} {} {} {} {} {} {}", search, pgCount, esCount,
                    ctxt.txc.txID, ctxt.txc.xmin, ctxt.txc.xmax, ctxt.txc.activeTransactions);
            return -1;
        }
    }
}
