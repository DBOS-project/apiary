package org.dbos.apiary.procedures.postgres.shop;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.SQLException;

public class ShopSearchItem extends PostgresFunction {

    public static String[] runFunction(PostgresContext ctxt, String searchText, int maxCost) throws Exception {
        return ctxt.apiaryCallFunction("ShopESSearchItem", searchText, maxCost).getStringArray();
    }
}
