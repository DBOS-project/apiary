package org.dbos.apiary.procedures.postgres.shop;

import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ShopCheckoutCart extends PostgresFunction {
    private static final String emptyCart = "DELETE FROM ShopCart WHERE PersonID=?;";
    private static final String addOrder = "INSERT INTO ShopOrders(PersonID, OrderID, ItemID) VALUES (?, ?, ?);";
    private static final String addTransaction = "INSERT INTO ShopTransactions(OrderID, PersonID, Cost) VALUES (?, ?, ?);";
    private static final String getCart = "SELECT ItemID, Cost FROM ShopCart WHERE PersonID=?";

    public static int runFunction(PostgresContext ctxt, int personID) throws SQLException {
        int orderID = (int) ctxt.txc.txID;
        ResultSet rs = ctxt.executeQuery(getCart, personID);
        int totalCost = 0;
        while (rs.next()) {
            int itemID = rs.getInt(1);
            int cost = rs.getInt(2);
            ctxt.executeUpdate(addOrder, personID, orderID, itemID);
            totalCost += cost;
        }
        ctxt.executeUpdate(addTransaction, orderID, personID, totalCost);
        ctxt.executeUpdate(emptyCart, personID);
        return totalCost;
    }
}
