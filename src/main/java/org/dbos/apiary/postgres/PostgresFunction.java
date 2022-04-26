package org.dbos.apiary.postgres;

import org.dbos.apiary.interposition.ApiaryFunction;
import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Utilities;

public class PostgresFunction implements ApiaryFunction {
    @Override
    public void recordInvocation(ApiaryFunctionContext ctxt, String funcName) {
        if (ctxt.provBuff == null) {
            // If no OLAP DB available.
            return;
        }
        long timestamp = Utilities.getMicroTimestamp();
        long txid = ((ApiaryStatefulFunctionContext) ctxt).getTransactionId();
        ctxt.provBuff.addEntry(ApiaryConfig.tableFuncInvocations, txid, timestamp, ctxt.execID, ctxt.service, funcName);
    }
}
