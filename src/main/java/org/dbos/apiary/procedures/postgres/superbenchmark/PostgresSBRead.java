package org.dbos.apiary.procedures.postgres.superbenchmark;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;

public class PostgresSBRead extends PostgresFunction {
    private static final String read = "SELECT Inventory FROM SuperbenchmarkTable WHERE ItemID=?;";

    public static int[] runFunction(PostgresContext ctxt, String itemName) throws Exception {
        int itemID = ctxt.apiaryCallFunction("ElasticsearchSBRead", itemName).getInt();
        ResultSet rs = ctxt.executeQuery(read, itemID);
        int inventory;
        if (rs.next()) {
            inventory = rs.getInt(1);
        } else {
            inventory = -1;
        }
        int cost = ctxt.apiaryCallFunction("MongoSBRead", itemID).getInt();
        return new int[]{itemID, inventory, cost};
    }
}
