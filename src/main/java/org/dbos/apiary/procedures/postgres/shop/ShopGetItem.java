package org.dbos.apiary.procedures.postgres.shop;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;

public class ShopGetItem extends PostgresFunction {

    public static int runFunction(PostgresContext ctxt, int personID, String searchText, int maxCost) throws SQLException {
        String bestItem = ctxt.apiaryCallFunction("ShopESSearchItem", searchText, maxCost).getStringArray()[0];
        ctxt.apiaryCallFunction("ShopAddCart", personID, Integer.parseInt(bestItem));
        return Integer.parseInt(bestItem);
    }
}
