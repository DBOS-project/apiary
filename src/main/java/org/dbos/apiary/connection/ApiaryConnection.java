package org.dbos.apiary.connection;

import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.ProvenanceBuffer;

import java.util.Map;

/**
 * A connection to a database.
 */
public interface ApiaryConnection {
    /**
     * For internal use only.
     * @param provBuff
     * @param service
     * @param execID
     * @param functionID
     * @param name
     * @param inputs
     * @return
     * @throws Exception
     */
    FunctionOutput callFunction(ProvenanceBuffer provBuff, String service, long execID, long functionID, String name, Object... inputs) throws Exception;

    // For partition mapping information.

    /**
     * For internal use only.
     */
    void updatePartitionInfo();

    /**
     * For internal use only.
     * @return
     */
    int getNumPartitions();

    /**
     * For internal use only.
     * @param input
     * @return
     */
    String getHostname(Object... input);  // Return the hostname to which to send an input.

    /**
     * For internal use only.
     * @return
     */
    Map<Integer, String> getPartitionHostMap();

}
