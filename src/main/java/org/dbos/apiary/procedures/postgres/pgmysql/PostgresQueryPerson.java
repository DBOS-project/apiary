package org.dbos.apiary.procedures.postgres.pgmysql;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.dbos.apiary.procedures.postgres.pgmongo.PostgresFindPerson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;

public class PostgresQueryPerson extends PostgresFunction {

    private static final Logger logger = LoggerFactory.getLogger(PostgresFindPerson.class);

    private final static String find = "SELECT COUNT(*) FROM PersonTable WHERE Name=?";

    public static int runFunction(PostgresContext ctxt, String search) throws Exception {
        ResultSet rs = ctxt.executeQuery(find, search);
        rs.next();
        int pgCount = rs.getInt(1);
        int mysqlCount = ctxt.apiaryCallFunction("MysqlQueryPerson", search).getInt();
        if (pgCount == mysqlCount) {
            return pgCount;
        } else {
            logger.info("Inconsistency: {} postgres: {} mysql: {} txID: {} xmin: {} xmax: {} activeTxns: {}", search, pgCount, mysqlCount,
                    ctxt.txc.txID, ctxt.txc.xmin, ctxt.txc.xmax, ctxt.txc.activeTransactions);
            return -1;
        }
    }
}
