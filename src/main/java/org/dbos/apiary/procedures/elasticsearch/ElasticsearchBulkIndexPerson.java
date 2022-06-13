package org.dbos.apiary.procedures.elasticsearch;

import org.dbos.apiary.elasticsearch.ApiaryDocument;
import org.dbos.apiary.elasticsearch.ElasticsearchContext;
import org.dbos.apiary.elasticsearch.ElasticsearchFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticsearchBulkIndexPerson extends ElasticsearchFunction {
    public int runFunction(ElasticsearchContext context, String[] names, int[] numbers) throws IOException {
        List<ApiaryDocument> people = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < names.length; i++) {
            Person person = new Person(names[i], numbers[i]);
            people.add(person);
            ids.add(names[i]);
        }
        context.executeBulkWrite("people", people, ids);
        return 0;
    }
}
