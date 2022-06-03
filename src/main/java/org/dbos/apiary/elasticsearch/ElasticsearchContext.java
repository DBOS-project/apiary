package org.dbos.apiary.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.WorkerContext;

import java.io.IOException;
import java.util.List;

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

    public void executeUpdate(String index, ApiaryDocument document, String id) {
        try {
            document.setId(id);
            document.setVersion(6);
            client.index(i -> i
                    .index(index)
                    .document(document)
                    .refresh(Refresh.WaitFor));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public SearchResponse executeQuery(String index, Query searchQuery, Class clazz) {
        try {
            SearchRequest request = SearchRequest.of(s -> s
                    .index(index).query(q -> q.bool(b -> b
                                    .must(searchQuery)
                                    .filter(List.of(
                                            BoolQuery.of(bb -> bb
                                                    .should(
                                                            RangeQuery.of(f -> f.field("version").lte(JsonData.of(5)))._toQuery(),
                                                            TermQuery.of(f -> f.field("version").value(6))._toQuery()
                                                    )
                                            )._toQuery())
                            )
                    ))
            );
            return client.search(request, clazz);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
