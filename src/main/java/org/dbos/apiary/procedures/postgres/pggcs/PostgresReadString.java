package org.dbos.apiary.procedures.postgres.pggcs;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;

public class PostgresReadString extends PostgresFunction {
    private static final Logger logger = LoggerFactory.getLogger(PostgresReadString.class);

    private final static String search = "SELECT Stuff FROM StuffTable WHERE Name=?";

    public int runFunction(PostgresContext context, String name) throws Exception {
        ResultSet rs = context.executeQuery(search, name);
        String pgStuff;
        if (rs.next()) {
            pgStuff = rs.getString(1);
        } else {
            pgStuff = "none";
        }
        String gcsStuff = context.apiaryCallFunction("GCSReadString", name).getString();
        if (pgStuff.equals(gcsStuff)) {
            return 0;
        } else {
            logger.info("{} {} {} {} {} {} {}", name, pgStuff, gcsStuff,
                    context.txc.txID, context.txc.xmin, context.txc.xmax, context.txc.activeTransactions);
            return -1;
        }
    }
}
