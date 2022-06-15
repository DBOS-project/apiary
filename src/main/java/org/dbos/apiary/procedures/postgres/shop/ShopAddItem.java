package org.dbos.apiary.procedures.postgres.shop;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class ShopAddItem extends PostgresFunction {
    private static final String addItem = "INSERT INTO ShopItems(ItemID, ItemName, ItemDesc, Cost, Inventory) VALUES (?, ?, ?, ?, ?) ON CONFLICT (ItemID) DO UPDATE SET Cost = EXCLUDED.Cost;";

    public static int runFunction(PostgresContext ctxt, int itemID, String itemName, String itemDesc, int cost, int inventory) throws Exception {
        ctxt.executeUpdate(addItem, itemID, itemName, itemDesc, cost, inventory);
        ctxt.apiaryCallFunction("ShopESAddItem", itemID, itemName, itemDesc, cost);
        return 0;
    }
}
