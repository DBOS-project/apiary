package org.dbos.apiary.procedures.postgres.superbenchmark;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresSBWrite extends PostgresFunction {
    private static final String insert = "INSERT INTO SuperbenchmarkTable(ItemID, Inventory) VALUES (?, ?);";

    public static int runFunction(PostgresContext ctxt, int itemID, String itemName, int cost, int inventory) throws Exception {
        ctxt.executeUpdate(insert, itemID, inventory);
        ctxt.apiaryCallFunction("ElasticsearchSBWrite", itemID, itemName);
        ctxt.apiaryCallFunction("MongoSBWrite", itemID, cost);
        return 0;
    }
}
