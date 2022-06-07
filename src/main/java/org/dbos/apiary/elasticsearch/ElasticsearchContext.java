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
import com.google.protobuf.Api;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticsearchContext extends ApiaryContext {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchContext.class);

    private final ElasticsearchClient client;
    private final TransactionContext txc;

    List<String> updatedKeys = new ArrayList<>();

    public ElasticsearchContext(ElasticsearchClient client, WorkerContext workerContext, TransactionContext txc, String service, long execID, long functionID) {
        super(workerContext, service, execID, functionID);
        this.client = client;
        this.txc = txc;
    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) {
        // TODO: Implement.
        return null;
    }

    public void executeUpdate(String index, ApiaryDocument document, String id) {
        try {
            updatedKeys.add(id);
            if (ApiaryConfig.XDBTransactions) {
                document.setApiaryID(id);
                document.setBeginVersion(txc.txID);
                document.setEndVersion(Long.MAX_VALUE);
            }
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
            if (ApiaryConfig.XDBTransactions) {
                List<Query> beginVersionFilter = new ArrayList<>();
                // If beginVersion is an active transaction, it is not in the snapshot.
                for (long txID : txc.activeTransactions) {
                    beginVersionFilter.add(TermQuery.of(f -> f.field("beginVersion").value(txID))._toQuery());
                }
                List<Query> endVersionFilter = new ArrayList<>();
                // If endVersion is greater than or equal to xmax, it is not in the snapshot.
                endVersionFilter.add(RangeQuery.of(f -> f.field("endVersion").gte(JsonData.of(txc.xmax)))._toQuery());
                // If endVersion is an active transaction, it is not in the snapshot.
                for (long txID : txc.activeTransactions) {
                    endVersionFilter.add(TermQuery.of(f -> f.field("endVersion").value(txID))._toQuery());
                }
                // TODO: Also handle records left by aborted transactions, which must be skipped.
                SearchRequest request = SearchRequest.of(s -> s
                        .index(index).query(q -> q.bool(b -> b
                                .must(searchQuery)
                                .filter(BoolQuery.of(bb -> bb
                                        .must( // beginVersion must be in the snapshot.
                                                RangeQuery.of(f -> f.field("beginVersion").lt(JsonData.of(txc.xmax)))._toQuery()
                                        ).mustNot( // Therefore, beginVersion must not be an active transaction.
                                                beginVersionFilter
                                        ).should( // endVersion must not be in the snapshot.
                                                endVersionFilter
                                        )
                                )._toQuery())
                        ))
                );
                return client.search(request, clazz);
            } else {
                SearchRequest request = SearchRequest.of(s -> s
                        .index(index).query(searchQuery));
                return client.search(request, clazz);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
