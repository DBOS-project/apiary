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
    public Map<Integer, String> getPartitionHostMap() {
        return Map.of(0, "localhost");
    }

    @Override
    public Map<Integer, Integer> getPartitionPkeyMap() {
        return Map.of(0, 0);
    }
}
