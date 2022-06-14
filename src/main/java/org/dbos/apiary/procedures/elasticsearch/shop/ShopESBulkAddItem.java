package org.dbos.apiary.procedures.elasticsearch.shop;

import org.dbos.apiary.elasticsearch.ApiaryDocument;
import org.dbos.apiary.elasticsearch.ElasticsearchContext;
import org.dbos.apiary.elasticsearch.ElasticsearchFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShopESBulkAddItem extends ElasticsearchFunction {
    public int runFunction(ElasticsearchContext context, int[] itemIDs, String[] itemNames, String[] itemDescs, int[] costs) throws IOException {
        List<ApiaryDocument> items = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < itemIDs.length; i++) {
            ShopItem item = new ShopItem(Integer.toString(itemIDs[i]), itemNames[i], itemDescs[i], costs[i]);
            items.add(item);
            ids.add(item.getItemID());
        }
        context.executeBulkWrite("items", items, ids);
        return 0;
    }
}
