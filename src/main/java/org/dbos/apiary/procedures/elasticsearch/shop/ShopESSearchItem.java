package org.dbos.apiary.procedures.elasticsearch.shop;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import org.dbos.apiary.elasticsearch.ElasticsearchContext;
import org.dbos.apiary.elasticsearch.ElasticsearchFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShopESSearchItem extends ElasticsearchFunction {

    public String[] runFunction(ElasticsearchContext context, String searchText, int maxCost) throws IOException {
        Query q = BoolQuery.of(b ->
                b.must(
                        MatchQuery.of(t -> t.field("itemName").query(searchText))._toQuery()
                ).filter(
                        RangeQuery.of(t -> t.field("cost").lte(JsonData.of(maxCost)))._toQuery()
                )
        )._toQuery();
        SearchResponse<ShopItem> response = context.executeQuery("items", q, ShopItem.class);
        List<String> itemIDs = new ArrayList<>();
        for (Hit<ShopItem> hit: response.hits().hits()) {
            ShopItem item = hit.source();
            assert item != null;
            itemIDs.add(item.itemID);
        }
        return itemIDs.toArray(new String[0]);
    }
}
