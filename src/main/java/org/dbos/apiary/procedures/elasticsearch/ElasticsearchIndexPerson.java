package org.dbos.apiary.procedures.elasticsearch;

import co.elastic.clients.elasticsearch.core.IndexRequest;
import org.dbos.apiary.elasticsearch.ElasticsearchContext;
import org.dbos.apiary.elasticsearch.ElasticsearchFunction;

public class ElasticsearchIndexPerson extends ElasticsearchFunction {
    public int runFunction(ElasticsearchContext context, String name, int number) {
        Person person = new Person(name, number);
        context.executeUpdate("people", person, name);
        return number;
    }
}
