package org.dbos.apiary.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.WorkerContext;

import java.io.IOException;

public class ElasticsearchContext extends ApiaryContext {
    private final ElasticsearchClient client;

    public ElasticsearchContext(ElasticsearchClient client, WorkerContext workerContext, String service, long execID, long functionID) {
        super(workerContext, service, execID, functionID);
        this.client = client;
    }

    @Override
    public FunctionOutput checkPreviousExecution() {
        return null;
    }

    @Override
    public void recordExecution(FunctionOutput output) {}

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) {
        // TODO: Implement.
        return null;
    }

    public void executeUpdate(IndexRequest indexRequest) {
        try {
            client.index(indexRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public SearchResponse executeQuery(SearchRequest request, Class clazz) {
        try {
            return client.search(request, clazz);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
