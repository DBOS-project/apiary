package org.dbos.apiary.elasticsearch;

public class ApiaryDocument {
    private String apiaryID;
    private long beginVersion;
    private long endVersion;


    public String getApiaryID() {
        return apiaryID;
    }

    public void setApiaryID(String apiaryID) {
        this.apiaryID = apiaryID;
    }

    public long getBeginVersion() {
        return beginVersion;
    }

    public void setBeginVersion(long beginVersion) {
        this.beginVersion = beginVersion;
    }

    public long getEndVersion() {
        return endVersion;
    }

    public void setEndVersion(long endVersion) {
        this.endVersion = endVersion;
    }
}