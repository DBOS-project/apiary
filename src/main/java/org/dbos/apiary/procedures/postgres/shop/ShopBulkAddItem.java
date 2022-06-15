package org.dbos.apiary.procedures.postgres.shop;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;

public class ShopBulkAddItem extends PostgresFunction {
    private static final String addItem = "INSERT INTO ShopItems(ItemID, ItemName, ItemDesc, Cost, Inventory) VALUES (?, ?, ?, ?, ?);";

    public static int runFunction(PostgresContext ctxt, int[] itemIDs, String[] itemNames, String[] itemDescs, int[] costs, int[] inventories) throws Exception {
        for (int i = 0; i < itemIDs.length; i++) {
            ctxt.executeUpdate(addItem, itemIDs[i], itemNames[i], itemDescs[i], costs[i], inventories[i]);
        }
        ctxt.apiaryCallFunction("ShopESBulkAddItem", itemIDs, itemNames, itemDescs, costs);
        return 0;
    }
}
