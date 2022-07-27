package org.dbos.apiary.procedures.elasticsearch.superbenchmark;

import org.dbos.apiary.elasticsearch.ElasticsearchContext;
import org.dbos.apiary.elasticsearch.ElasticsearchFunction;
import org.dbos.apiary.procedures.elasticsearch.shop.ShopItem;
import org.postgresql.util.PSQLException;

import java.io.IOException;

public class ElasticsearchSBWrite extends ElasticsearchFunction {
    public int runFunction(ElasticsearchContext context, int itemID, String itemName) throws PSQLException, IOException {
        SBItem item = new SBItem(Integer.toString(itemID), itemName);
        context.executeWrite("superbenchmark", item, item.getItemID());
        return 0;
    }
}
