package org.dbos.apiary.procedures.elasticsearch;

import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import org.dbos.apiary.elasticsearch.ElasticsearchContext;
import org.dbos.apiary.elasticsearch.ElasticsearchFunction;

public class ElasticsearchSearchPerson extends ElasticsearchFunction {

    public int runFunction(ElasticsearchContext context, String searchText) {
        SearchRequest request =
                new SearchRequest.Builder().index("people")
                        .query(q -> q
                                .match(t -> t
                                        .field("name")
                                        .query(searchText)
                                )
                        ).build();
        SearchResponse<Person> response = (SearchResponse<Person>) context.apiaryExecuteQuery(request, Person.class);
        return (int) response.hits().total().value();
    }
}
