package org.dbos.apiary.procedures.elasticsearch;

import org.dbos.apiary.elasticsearch.ElasticsearchContext;
import org.dbos.apiary.elasticsearch.ElasticsearchFunction;
import org.postgresql.util.PSQLException;

import java.io.IOException;

public class ElasticsearchIndexPerson extends ElasticsearchFunction {
    public int runFunction(ElasticsearchContext context, String name, int number) throws PSQLException, IOException {
        Person person = new Person(name, number);
        context.executeWrite("people", person, name);
        return number;
    }
}
