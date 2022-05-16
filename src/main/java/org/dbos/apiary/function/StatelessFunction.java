package org.dbos.apiary.function;

import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;

/**
 * The base class for Apiary stateless functions.
 */
public class StatelessFunction implements ApiaryFunction {

    @Override
    public void recordInvocation(ApiaryContext ctxt, String funcName) {
        if (ctxt.provBuff == null) {
            // If no OLAP DB available.
            return;
        }
        long timestamp = Utilities.getMicroTimestamp();
        ctxt.provBuff.addEntry(ApiaryConfig.tableFuncInvocations, ApiaryConfig.statelessTxid, timestamp, ctxt.execID, ctxt.service, funcName);
    }
}
