package org.dbos.apiary.procedures.postgres.superbenchmark;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresSBBulkWrite extends PostgresFunction {
    private static final String insert = "INSERT INTO SuperbenchmarkTable(ItemID, Inventory) VALUES (?, ?);";

    public static int runFunction(PostgresContext ctxt, int[] itemIDs, String[] itemNames, int[] costs, int[] inventories) throws Exception {
        for (int i = 0; i < itemIDs.length; i++) {
            ctxt.executeUpdate(insert, itemIDs[i], inventories[i]);
        }
        ctxt.apiaryCallFunction("ElasticsearchSBBulkWrite", itemIDs, itemNames);
        ctxt.apiaryCallFunction("MongoSBBulkWrite", itemIDs, costs);
        return 0;
    }
}
