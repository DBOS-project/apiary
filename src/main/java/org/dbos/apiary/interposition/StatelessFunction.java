package org.dbos.apiary.interposition;

import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * For internal use only.
 */
public class StatelessFunction implements ApiaryFunction {

    @Override
    public void recordInvocation(ApiaryFunctionContext ctxt, String funcName) {
        if (ctxt.provBuff == null) {
            // If no OLAP DB available.
            return;
        }
        long timestamp = Utilities.getMicroTimestamp();
        ctxt.provBuff.addEntry(ApiaryConfig.tableFuncInvocations, ApiaryConfig.statelessTxid, timestamp, ctxt.execID, ctxt.service, funcName);
    }
}
