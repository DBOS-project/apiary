package org.dbos.apiary.executor;

import java.util.Map;

public interface ApiaryConnection {
    FunctionOutput callFunction(String name, Object... inputs) throws Exception;

    // For partition mapping information.
    void updatePartitionInfo();
    int getNumPartitions();
    int getPartition(Object[] input);  // Return the partition to which to send an input.
    String getHostname(Object[] input);  // Return the hostname to which to send an input.
    Map<Integer, String> getPartitionHostMap();
}
