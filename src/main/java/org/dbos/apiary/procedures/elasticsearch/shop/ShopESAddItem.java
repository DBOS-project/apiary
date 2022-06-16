package org.dbos.apiary.procedures.elasticsearch.shop;

import org.dbos.apiary.elasticsearch.ElasticsearchContext;
import org.dbos.apiary.elasticsearch.ElasticsearchFunction;

public class ShopESAddItem extends ElasticsearchFunction {
    public int runFunction(ElasticsearchContext context, int itemID, String itemName, String itemDesc, int cost) {
        ShopItem item = new ShopItem(Integer.toString(itemID), itemName, itemDesc, cost);
        context.executeWrite("items", item, item.getItemID());
        return 0;
    }
}
