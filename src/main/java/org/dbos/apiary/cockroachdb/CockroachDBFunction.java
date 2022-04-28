package org.dbos.apiary.cockroachdb;

import org.dbos.apiary.interposition.ApiaryFunction;
import org.dbos.apiary.interposition.ApiaryFunctionContext;
import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;

public class CockroachDBFunction implements ApiaryFunction {
    @Override
    public void recordInvocation(ApiaryFunctionContext ctxt, String funcName) {
        // TODO: implement the logging.
    }
}
