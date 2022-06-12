package org.dbos.apiary.elasticsearch;

import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.ApiaryFunction;
import org.dbos.apiary.function.FunctionOutput;

public class ElasticsearchFunction implements ApiaryFunction {

    @Override
    public FunctionOutput apiaryRunFunction(ApiaryContext apiaryContext, Object... input) throws Exception {
        ElasticsearchContext ctxt = (ElasticsearchContext) apiaryContext;
        FunctionOutput fo = ApiaryFunction.super.apiaryRunFunction(apiaryContext, input);
        fo.setWrittenKeys(ctxt.writtenKeys);
        return fo;
    }

    @Override
    public void recordInvocation(ApiaryContext ctxt, String funcName) {

    }
}
