package org.dbos.apiary.function;

import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;

/**
 * The base class for Apiary stateless functions.
 */
public class StatelessFunction implements ApiaryFunction {

    @Override
    public void recordInvocation(ApiaryContext ctxt, String funcName) {
        if (ctxt.workerContext.provBuff == null) {
            // If no OLAP DB available.
            return;
        }
        long timestamp = Utilities.getMicroTimestamp();
        ctxt.workerContext.provBuff.addEntry(ApiaryConfig.tableFuncInvocations, ApiaryConfig.statelessTxid, timestamp, ctxt.execID, ctxt.role, funcName, /*readonly=*/true);
    }
}
