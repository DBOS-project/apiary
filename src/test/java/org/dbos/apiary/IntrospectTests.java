package org.dbos.apiary;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.voltdb.VoltDBConnection;
import org.dbos.apiary.voltdb.VoltPartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.ProcCallException;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IntrospectTests {
    private static final Logger logger = LoggerFactory.getLogger(IntrospectTests.class);

    @BeforeEach
    public void truncateTables() throws IOException, ProcCallException {
        VoltDBConnection ctxt = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        ctxt.client.callProcedure("TruncateTables");
    }

    @Test
    public void testVoltPartitionInfo() throws IOException, ProcCallException {
        logger.info("testVoltPartitionInfo");
        ApiaryConnection ctxt = new VoltDBConnection("localhost", ApiaryConfig.voltdbPort);
        VoltPartitionInfo vpi = new VoltPartitionInfo((VoltDBConnection) ctxt);
        int numPartitions = vpi.getNumPartitions();
        logger.info("Detected {} partitions.", numPartitions);
        assertTrue(numPartitions > 0);

        Map<Integer, String> partitionHostMap = vpi.getPartitionHostMap();
        for (int p : partitionHostMap.keySet()) {
            String hn = partitionHostMap.get(p);
            assertNotEquals("localhost", hn);
            logger.info("partition {} --> host {}", p, hn);
        }
        assertEquals(numPartitions, partitionHostMap.size());

        Map<Integer, Integer> partitionPkeyMap = vpi.getPartitionPkeyMap();
        for (int p : partitionPkeyMap.keySet()) {
            int pkey = partitionPkeyMap.get(p);
            assertTrue(p >= 0);
            assertTrue( pkey >= 0);
            logger.info("partition {} --> pkey {}", p, pkey);
        }

        // Update and test again.
        int numPartitions2 = vpi.updatePartitionInfo();
        assertEquals(numPartitions, numPartitions2);
        Map<Integer, String> partitionHostMap2 = vpi.getPartitionHostMap();
        assertTrue(partitionHostMap.equals(partitionHostMap2));
        Map<Integer, Integer> partitionPkeyMap2 = vpi.getPartitionPkeyMap();
        assertTrue(partitionPkeyMap.equals(partitionPkeyMap2));
    }
}
