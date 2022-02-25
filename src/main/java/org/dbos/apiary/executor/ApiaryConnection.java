package org.dbos.apiary.executor;

import java.util.Map;

public interface ApiaryConnection {
    FunctionOutput callFunction(String name, int pkey, Object... inputs) throws Exception;

    // For partition mapping information.
    void updatePartitionInfo();
    int getNumPartitions();
    String getHostname(int pkey);  // Return the hostname of a given pkey.
    Map<Integer, String> getPartitionHostMap();
}
