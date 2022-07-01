package org.dbos.apiary.procedures.elasticsearch.shop;

import org.dbos.apiary.elasticsearch.ElasticsearchContext;
import org.dbos.apiary.elasticsearch.ElasticsearchFunction;
import org.postgresql.util.PSQLException;

import java.io.IOException;

public class ShopESAddItem extends ElasticsearchFunction {
    public int runFunction(ElasticsearchContext context, int itemID, String itemName, String itemDesc, int cost) throws PSQLException, IOException {
        ShopItem item = new ShopItem(Integer.toString(itemID), itemName, itemDesc, cost);
        context.executeWrite("items", item, item.getItemID());
        return 0;
    }
}
