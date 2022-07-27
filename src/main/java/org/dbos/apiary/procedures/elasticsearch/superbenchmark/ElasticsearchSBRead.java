package org.dbos.apiary.procedures.elasticsearch.superbenchmark;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import org.dbos.apiary.elasticsearch.ElasticsearchContext;
import org.dbos.apiary.elasticsearch.ElasticsearchFunction;

import java.io.IOException;
import java.util.List;

public class ElasticsearchSBRead extends ElasticsearchFunction {

    public int runFunction(ElasticsearchContext context, String searchText) throws IOException {
        Query q = BoolQuery.of(b ->
                b.must(
                        MatchQuery.of(t -> t.field("itemName").query(searchText))._toQuery()
                )
        )._toQuery();
        SearchResponse<SBItem> response = context.executeQuery("superbenchmark", q, SBItem.class);
        List<Hit<SBItem>> hits = response.hits().hits();
        if (hits.size() > 0) {
            return Integer.parseInt(hits.get(0).source().itemID);
        } else {
            return -1;
        }
    }
}
