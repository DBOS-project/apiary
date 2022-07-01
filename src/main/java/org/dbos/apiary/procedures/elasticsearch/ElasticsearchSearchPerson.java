package org.dbos.apiary.procedures.elasticsearch;

import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import org.dbos.apiary.elasticsearch.ElasticsearchContext;
import org.dbos.apiary.elasticsearch.ElasticsearchFunction;

import java.io.IOException;

public class ElasticsearchSearchPerson extends ElasticsearchFunction {

    public int runFunction(ElasticsearchContext context, String searchText) throws IOException {
        Query q = MatchQuery.of(t -> t.field("name").query(searchText))._toQuery();
        SearchResponse<Person> response = context.executeQuery("people", q, Person.class);
        return (int) response.hits().total().value();
    }
}
