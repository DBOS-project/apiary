package org.dbos.apiary.procedures.postgres.shop;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ShopAddCart extends PostgresFunction {
    private static final String getCost = "SELECT Cost FROM ShopItems WHERE ItemID=?";
    private static final String addCart = "INSERT INTO ShopCart(PersonID, ItemID, Cost) VALUES (?, ?, ?);";

    public static int runFunction(PostgresContext ctxt, int personID, int itemID) throws SQLException {
        ResultSet rs = ctxt.executeQuery(getCost, itemID);
        rs.next();
        int cost = rs.getInt(1);
        ctxt.executeUpdate(addCart, personID, itemID, cost);
        return 0;
    }
}
