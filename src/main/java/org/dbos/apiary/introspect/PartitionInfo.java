package org.dbos.apiary.introspect;

import org.dbos.apiary.executor.ApiaryConnection;

import java.util.Map;

public interface PartitionInfo {
    int updatePartitionInfo();  // Return number of partitions.
    int getNumPartitions();
    Map<Integer, String> getPartitionHostMap();
    Map<Integer, Integer> getPartitionPkeyMap();
}
