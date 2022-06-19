package org.dbos.apiary.procedures.postgres.shop;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ShopAddCart extends PostgresFunction {
    private static final String getItem = "SELECT Cost, Inventory FROM ShopItems WHERE ItemID=?";
    private static final String addCart = "INSERT INTO ShopCart(PersonID, ItemID, Cost) VALUES (?, ?, ?);";
    private static final String updateInventory = "UPDATE ShopItems SET Inventory=? WHERE ItemID=?";

    public static int runFunction(PostgresContext ctxt, int personID, int itemID) throws SQLException {
        ResultSet rs = ctxt.executeQuery(getItem, itemID);
        if (rs.next()) {
            int cost = rs.getInt(1);
            int inventory = rs.getInt(2);
            assert (inventory >= 0);
            if (inventory == 0) {
                return 1;
            }
            ctxt.executeUpdate(addCart, personID, itemID, cost);
            ctxt.executeUpdate(updateInventory, inventory - 1, itemID);
            return 0;
        } else {
            return -1;
        }
    }
}
