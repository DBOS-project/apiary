package org.dbos.apiary.voltdb;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.introspect.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.VoltTable;
import org.voltdb.client.ProcCallException;
import org.voltdb.iv2.MpInitiator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class VoltPartitionInfo implements PartitionInfo {
    private static final Logger logger = LoggerFactory.getLogger(VoltPartitionInfo.class);

    // NOTE: the table name must be all UPPERCASE!
    public static String kPartitionInfoTableName = "PARTITIONINFO";

    private final VoltDBConnection ctxt;
    private final Map<Integer, String> partitionHostMap = new HashMap<>();
    private final Map<Integer, Integer> partitionPkeyMap = new HashMap<>();
    private final Map<Integer, String> hostIdNameMap = new HashMap<>();
    private int numPartitions;

    public VoltPartitionInfo(VoltDBConnection ctxt) {
        this.ctxt = ctxt;
        this.numPartitions = updatePartitionInfo();
    }

    // Update partition info table: (partitionID, pkey, hostId, hostname, isLeader).
    @Override
    public int updatePartitionInfo() {
        int numPkeys, numSites, numLeaders;
        partitionHostMap.clear();
        partitionPkeyMap.clear();
        hostIdNameMap.clear();
        try {
            // Also update hostIdNameMap.
            numSites = updateHostMap();
            if (numSites < 0) { return -1; }
            // Also update partitionPkeyMap
            numPkeys = updatePartitionPkeyMap();
            if (numPkeys < 0) { return -1; }
            // Also update partitionHostMap;
            numLeaders = updatePartitionLeader();
            if (numLeaders < 0) { return -1; }
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        if (((numSites % numPkeys) == 0) && (numLeaders == numPkeys)) {
            return numPkeys;
        }
        return -1;
    }

    @Override
    public Map<Integer, String> getPartitionHostMap() {
        return this.partitionHostMap;
    }

    @Override
    public Map<Integer, Integer> getPartitionPkeyMap() {
        return this.partitionPkeyMap;
    }

    @Override
    public int getNumPartitions() {
        return this.numPartitions;
    }

     // Update hostId and hostname info.
     // Return number of rows in map on success, return -1 on failure.
    private int updateHostMap() throws IOException, ProcCallException {
        // 1) Get the statistics for a table.
        String sqlQuery = new StringBuilder()
                .append("SELECT partition_id, host_id, hostname")
                .append(String.format(" FROM STATISTICS(TABLE, 0) where TABLE_NAME = '%s';",
                        kPartitionInfoTableName)).toString();
        VoltTable hostMap = ctxt.client.callProcedure("@QueryStats", sqlQuery).getResults()[0];

        // 2) Update the PartitionInfo table with host info.
        int rowCnt = 0;
        while (hostMap.advanceRow()) {
            int hostId = (int) hostMap.getLong(1);
            String hostName = hostMap.getString(2);
            hostIdNameMap.put(hostId, hostName);
            // TODO: actually update the partitionInfo table?
            rowCnt++;
        }
        return rowCnt;
    }

    // Update partitionID -> pkey mapping to the PartitionInfo table.
    // Also store the mapping in local variable.
    // Return number of rows in map on success, return -1 on failure.
    private int updatePartitionPkeyMap() throws IOException, ProcCallException {
        // 1) Get the partitionID, pkey mapping.
        VoltTable vPkeyMap = ctxt.client.callProcedure("@GetPartitionKeys",
                "Integer").getResults()[0];

        // 2) Update the partitionPkeyMap.
        int rowCnt = 0;
        while (vPkeyMap.advanceRow()) {
            int partitionId = (int) vPkeyMap.getLong(0);
            int pkey = (int) vPkeyMap.getLong(1);
            this.partitionPkeyMap.put(partitionId, pkey);
            rowCnt++;
        }
        return rowCnt;
    }

    // Update which host is the leader for each partition.
    // Return number of leaders in map on success, return -1 on failure.
    private int updatePartitionLeader() throws IOException, ProcCallException {
        // 1) Get the partitionID, leader mapping.
        VoltTable partitionLeaderMap = ctxt.client.callProcedure("@Statistics", "TOPO", 0).getResults()[0];

        // 2) Insert into the partitionHostMap.
        int rowCnt = 0;
        while (partitionLeaderMap.advanceRow()) {
            // Reference: https://github.com/VoltDB/voltdb/blob/master/tests/frontend/org/voltdb/regressionsuites/statistics/TestStatisticsSuite.java#L556
            int partitionId = (int) partitionLeaderMap.getLong(0);
            if (partitionId >= MpInitiator.MP_INIT_PID) {
                // Skip this special row.
                continue;
            }
            String leader = partitionLeaderMap.getString(2);
            // Parse the string, it will be hostID:siteID.
            String[] hostSite = leader.split(":");
            int hostId = Integer.parseInt(hostSite[0]);
            String hostname = hostIdNameMap.get(hostId);
            this.partitionHostMap.put(partitionId, hostname);
            rowCnt++;
        }
        return rowCnt;
    }
}
