package org.dbos.apiary.procedures.elasticsearch;

import org.dbos.apiary.elasticsearch.ElasticsearchContext;
import org.dbos.apiary.elasticsearch.ElasticsearchFunction;

public class ElasticsearchIndexPerson extends ElasticsearchFunction {
    public int runFunction(ElasticsearchContext context, String name, int number) {
        Person person = new Person(name, number);
        context.executeWrite("people", person, name);
        return number;
    }
}
