package org.dbos.apiary.executor;

import org.dbos.apiary.interposition.ProvenanceBuffer;

import java.util.Map;

public interface ApiaryConnection {
    FunctionOutput callFunction(ProvenanceBuffer provBuff, String service, long execID, long functionID, String name, Object... inputs) throws Exception;

    // For partition mapping information.
    void updatePartitionInfo();
    int getNumPartitions();
    String getHostname(Object... input);  // Return the hostname to which to send an input.
    Map<Integer, String> getPartitionHostMap();
}
