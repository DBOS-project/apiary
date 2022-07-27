package org.dbos.apiary.procedures.elasticsearch.superbenchmark;

import org.dbos.apiary.elasticsearch.ApiaryDocument;

public class SBItem extends ApiaryDocument {

    public SBItem() {}
    public SBItem(String itemID, String itemName) {
        this.itemID = itemID;
        this.itemName = itemName;
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
}
