package org.dbos.apiary.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import org.dbos.apiary.function.ApiaryTransactionalContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.ProvenanceBuffer;

import java.io.IOException;

public class ElasticsearchContext extends ApiaryTransactionalContext {
    private final ElasticsearchClient client;

    public ElasticsearchContext(ElasticsearchClient client, ProvenanceBuffer provBuff, String service, long execID, long functionID) {
        super(provBuff, service, execID, functionID);
        this.client = client;
    }

    @Override
    public FunctionOutput checkPreviousExecution() {
        return null;
    }

    @Override
    public void recordExecution(FunctionOutput output) {

    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) {
        return null;
    }

    @Override
    protected void internalExecuteUpdate(Object indexRequest, Object... input) {
        try {
            client.index((IndexRequest) indexRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void internalExecuteUpdateCaptured(Object procedure, Object... input) {

    }

    @Override
    protected SearchResponse internalExecuteQuery(Object procedure, Object... input) {
        try {
            return client.search((SearchRequest) procedure, (Class) input[0]);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected Object internalExecuteQueryCaptured(Object procedure, Object... input) {
        return null;
    }

    @Override
    protected long internalGetTransactionId() {
        return 0;
    }
}
