package org.dbos.apiary.postgres;

import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.ApiaryFunction;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All Postgres functions should extend this class and implement <code>runFunction</code>.
 */
public class PostgresFunction implements ApiaryFunction {
    private static final Logger logger = LoggerFactory.getLogger(PostgresFunction.class);

    @Override
    public void recordInvocation(ApiaryContext ctxt, String funcName) {
        // TODO: add replay flag.
        short isreplay = 0;
        if (ctxt.workerContext.provBuff == null) {
            // If no OLAP DB available.
            return;
        }
        long timestamp = Utilities.getMicroTimestamp();
        long txid = ((PostgresContext) ctxt).txc.txID;
        ctxt.workerContext.provBuff.addEntry(ApiaryConfig.tableFuncInvocations, txid, timestamp, ctxt.execID, isreplay, ctxt.service, funcName);
    }
}
