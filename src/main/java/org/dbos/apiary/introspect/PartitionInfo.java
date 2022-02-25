package org.dbos.apiary.introspect;

import java.util.Map;

public interface PartitionInfo {
    int updatePartitionInfo();  // Return number of partitions.
    int getNumPartitions();
    String getHostname(int pkey);  // Return the hostname of a given pkey.
    Map<Integer, String> getPartitionHostMap();
}
