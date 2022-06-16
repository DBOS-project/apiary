package org.dbos.apiary.procedures.elasticsearch.shop;

import org.dbos.apiary.elasticsearch.ApiaryDocument;

public class ShopItem extends ApiaryDocument {

    public ShopItem() {}
    public ShopItem(String itemID, String itemName, String itemDesc, int cost) {
        this.itemID = itemID;
        this.itemName = itemName;
        this.itemDesc = itemDesc;
        this.cost = cost;
    }

    public String getItemID() {
        return itemID;
    }

    public void setItemID(String itemID) {
        this.itemID = itemID;
    }

    String itemID;

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    String itemName;

    public String getItemDesc() {
        return itemDesc;
    }

    public void setItemDesc(String itemDesc) {
        this.itemDesc = itemDesc;
    }

    String itemDesc;

    public int getCost() {
        return cost;
    }

    public void setCost(int cost) {
        this.cost = cost;
    }

    int cost;
}
