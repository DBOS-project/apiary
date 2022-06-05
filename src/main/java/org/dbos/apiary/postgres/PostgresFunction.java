package org.dbos.apiary.postgres;

import org.dbos.apiary.function.ApiaryFunction;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;

import java.sql.SQLException;
import java.util.List;

/**
 * All Postgres functions should extend this class and implement <code>runFunction</code>.
 */
public class PostgresFunction implements ApiaryFunction {

    @Override
    public void recordInvocation(ApiaryContext ctxt, String funcName) {
        if (ctxt.workerContext.provBuff == null) {
            // If no OLAP DB available.
            return;
        }
        long timestamp = Utilities.getMicroTimestamp();
        long txid = ((PostgresContext) ctxt).txc.txID;
        ctxt.workerContext.provBuff.addEntry(ApiaryConfig.tableFuncInvocations, txid, timestamp, ctxt.execID, ctxt.service, funcName);
    }
}
