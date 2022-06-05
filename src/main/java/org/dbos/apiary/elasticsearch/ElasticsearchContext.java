package org.dbos.apiary.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticsearchContext extends ApiaryContext {
    private final ElasticsearchClient client;
    private final TransactionContext txc;

    public ElasticsearchContext(ElasticsearchClient client, WorkerContext workerContext, TransactionContext txc, String service, long execID, long functionID) {
        super(workerContext, service, execID, functionID);
        this.client = client;
        this.txc = txc;
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
            document.setApiaryID(id);
            document.setBeginVersion(txc.txID);
            document.setEndVersion(Long.MAX_VALUE);
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
            List<Query> activeTransactionsFilter = new ArrayList<>();
            for (long txID: txc.activeTransactions) {
                activeTransactionsFilter.add(TermQuery.of(f -> f.field("beginVersion").value(txID))._toQuery());
            }
            SearchRequest request = SearchRequest.of(s -> s
                    .index(index).query(q -> q.bool(b -> b
                                    .must(searchQuery)
                                    .filter(BoolQuery.of(bb -> bb
                                                    .must(
                                                            RangeQuery.of(f -> f.field("beginVersion").lte(JsonData.of(txc.xmax)))._toQuery()
                                                    ).mustNot(
                                                            activeTransactionsFilter
                                                    )
                                            )._toQuery())
                    ))
            );
            return client.search(request, clazz);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
