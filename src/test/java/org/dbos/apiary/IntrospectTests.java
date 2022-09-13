package org.dbos.apiary;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.voltdb.VoltConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.ProcCallException;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IntrospectTests {
    private static final Logger logger = LoggerFactory.getLogger(IntrospectTests.class);

    @BeforeEach
    public void truncateTables() throws IOException, ProcCallException {
        try {
            VoltConnection ctxt = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
            ctxt.client.callProcedure("TruncateTables");
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to connect to VoltDB.");
        }
    }

    @Test
    public void testVoltPartitionInfo() throws IOException {
        logger.info("testVoltPartitionInfo");
        VoltConnection ctxt;
        try {
            ctxt = new VoltConnection("localhost", ApiaryConfig.voltdbPort);
        } catch (Exception e) {
            logger.info("No VoltDB instance!");
            return;
        }
        int numPartitions = ctxt.getNumPartitions();
        logger.info("Detected {} partitions.", numPartitions);
        assertTrue(numPartitions > 0);

        HashMap<Integer, String> partitionHostMap = (HashMap)((HashMap)ctxt.getPartitionHostMap()).clone();
        String localhost = InetAddress.getLocalHost().getHostName();
        for (int p : partitionHostMap.keySet()) {
            String hn = partitionHostMap.get(p);
            assertEquals(localhost, hn);
            logger.info("partition {} --> host {}", p, hn);
        }
        assertEquals(numPartitions, partitionHostMap.size());

        // Update and test again.
        ctxt.updatePartitionInfo();
        int numPartitions2 = ctxt.getNumPartitions();
        assertEquals(numPartitions, numPartitions2);
        Map<Integer, String> partitionHostMap2 = ctxt.getPartitionHostMap();
        assertTrue(partitionHostMap.equals(partitionHostMap2));
    }
}
