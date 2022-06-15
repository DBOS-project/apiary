package org.dbos.apiary.procedures.postgres.shop;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ShopGetItem extends PostgresFunction {

    public static int runFunction(PostgresContext ctxt, int personID, String searchText, int maxCost) throws Exception {
        String[] items = ctxt.apiaryCallFunction("ShopESSearchItem", searchText, maxCost).getStringArray();
        if (items.length > 0) {
            int bestItem = Integer.parseInt(items[0]);
            ctxt.apiaryCallFunction("ShopAddCart", personID, bestItem);
            return bestItem;
        } else {
            return -1;
        }
    }
}
