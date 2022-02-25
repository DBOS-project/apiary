package org.dbos.apiary.sqlite;

import org.dbos.apiary.introspect.PartitionInfo;

import java.util.Map;

public class SQLitePartitionInfo implements PartitionInfo {
    @Override
    public int updatePartitionInfo() {
        return 1;
    }

    @Override
    public int getNumPartitions() {
        return 1;
    }

    @Override
    public String getHostname(int pkey) {
        return "localhost";
    }

    @Override
    public Map<Integer, String> getPartitionHostMap() {
        return null;
    }

}
