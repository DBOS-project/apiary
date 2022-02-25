package org.dbos.apiary.introspect;

import org.dbos.apiary.executor.ApiaryConnection;

import java.util.Map;

public interface PartitionInfo {
    int updatePartitionInfo();  // Return number of partitions.
    int getNumPartitions();
    public String getHostname(int pkey);  // Return the hostname of a given pkey.
}
