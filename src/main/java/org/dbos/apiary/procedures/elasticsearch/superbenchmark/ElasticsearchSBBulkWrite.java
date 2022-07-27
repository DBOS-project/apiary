package org.dbos.apiary.procedures.elasticsearch.superbenchmark;

import org.dbos.apiary.elasticsearch.ApiaryDocument;
import org.dbos.apiary.elasticsearch.ElasticsearchContext;
import org.dbos.apiary.elasticsearch.ElasticsearchFunction;
import org.postgresql.util.PSQLException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticsearchSBBulkWrite extends ElasticsearchFunction {
    public int runFunction(ElasticsearchContext context, int[] itemIDs, String[] itemNames) throws PSQLException, IOException {
        List<ApiaryDocument> items = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < itemIDs.length; i++) {
            SBItem item = new SBItem(Integer.toString(itemIDs[i]), itemNames[i]);
            items.add(item);
            ids.add(item.getItemID());
        }
        context.executeBulkWrite("superbenchmark", items, ids);
        return 0;
    }
}
