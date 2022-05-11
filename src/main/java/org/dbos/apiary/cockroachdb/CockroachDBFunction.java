package org.dbos.apiary.cockroachdb;

import org.dbos.apiary.function.ApiaryFunction;
import org.dbos.apiary.function.ApiaryContext;

public class CockroachDBFunction implements ApiaryFunction {
    @Override
    public void recordInvocation(ApiaryContext ctxt, String funcName) {
        // TODO: implement the logging.
    }
}
