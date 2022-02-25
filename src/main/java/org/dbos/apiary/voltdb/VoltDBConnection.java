package org.dbos.apiary.voltdb;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.executor.Task;
import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;
import org.voltdb.iv2.MpInitiator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VoltDBConnection implements ApiaryConnection {
    private static final Logger logger = LoggerFactory.getLogger(VoltDBConnection.class);
    public final Client client;
    public static String kPartitionInfoTableName = "PARTITIONINFO";
    private final Map<Integer, String> partitionHostMap = new HashMap<>();
    private final Map<Integer, String> hostIdNameMap = new HashMap<>();
    private int numPartitions;

    public VoltDBConnection(String hostname, Integer port) throws IOException {
        ClientConfig config = new ClientConfig();
        this.client = ClientFactory.createClient(config);
        client.createConnection(hostname, port);
        updatePartitionInfo();
    }

    private static VoltTable inputToVoltTable(Object... inputs) {
        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[inputs.length];
        for (int i = 0; i < inputs.length; i++) {
            Object input = inputs[i];
            columns[i] = VoltUtilities.objectToColumnInfo(i, input);
        }
        VoltTable v = new VoltTable(columns);
        Object[] row = new Object[v.getColumnCount()];
        for (int i = 0; i < inputs.length; i++) {
            Object input = inputs[i];
            if (input instanceof String[]) {
                row[i] = Utilities.stringArraytoByteArray((String[]) input);
            } else if (input instanceof Integer) {
                row[i] = input;
            } else if (input instanceof Double) {
                row[i] = input;
            } else if (input instanceof String) {
                row[i] = input;
            } else {
                logger.error("Do not support input type: {}, in parameter index {}", input.getClass().getName(), i);
                return null;
            }
        }
        v.addRow(row);
        return v;
    }

    private static Task voltOutputToTask(VoltTable voltInput) {
        VoltTableRow inputRow = voltInput.fetchRow(0);
        String funcName = inputRow.getString(0);
        int taskID = (int) inputRow.getLong(1);
        int pkey = (int) inputRow.getLong(2);
        Object[] input = new Object[voltInput.getColumnCount() - 3];

        int objIndex = 0;
        for (int i = 3; i < voltInput.getColumnCount(); i++, objIndex++) {
            VoltType t = inputRow.getColumnType(i);
            if (t.equals(VoltType.BIGINT)) {
                input[objIndex] = (int) inputRow.getLong(i);
            } else if (t.equals(VoltType.FLOAT)) {
                input[objIndex] = inputRow.getDouble(i);
            } else if (t.equals(VoltType.STRING)) {
                input[objIndex] = inputRow.getString(i);
            } else if (t.equals(VoltType.VARBINARY)) {
                input[objIndex] = Utilities.byteArrayToStringArray(inputRow.getVarbinary(i));
            } else if (t.equals(VoltType.SMALLINT)) {
                int futureID = (int) inputRow.getLong(i);
                input[objIndex] = new ApiaryFuture(futureID);
            } else {
                logger.error("Cannot support object type {}, index {}", t.getName(), objIndex);
                throw new IllegalArgumentException();
            }
        }
        return new Task(taskID, funcName, pkey, input);
    }

    @Override
    public FunctionOutput callFunction(String funcName, int pkey, Object... inputs) throws IOException, ProcCallException {
        VoltTable voltInput = inputToVoltTable(inputs);
        VoltTable[] res  = client.callProcedure(funcName, pkey, voltInput).getResults();
        VoltTable retVal = res[0];
        assert (retVal.getColumnCount() == 1 && retVal.getRowCount() == 1);
        String stringOutput = null;
        ApiaryFuture futureOutput = null;
        if (retVal.getColumnType(0).equals(VoltType.STRING)) { // Handle a string output.
            stringOutput = retVal.fetchRow(0).getString(0);
        } else { // Handle a future output.
            assert (retVal.getColumnType(0).equals(VoltType.SMALLINT));
            int futureID = (int) retVal.fetchRow(0).getLong(0);
            futureOutput = new ApiaryFuture(futureID);
        }
        List<Task> calledFunctions = new ArrayList<>();
        for (int i = 1; i < res.length; i++) {
            calledFunctions.add(voltOutputToTask(res[i]));
        }
        return new FunctionOutput(stringOutput, futureOutput, calledFunctions);
    }

    // Update partition info table: (partitionID, pkey, hostId, hostname, isLeader).
    // Warning: this function is not thread safe.
    @Override
    public void updatePartitionInfo() {
        int numSites = -1;
        int numLeaders = -1;
        partitionHostMap.clear();
        hostIdNameMap.clear();
        try {
            // Also update hostIdNameMap.
            numSites = updateHostMap();
            // Also update partitionHostMap;
            numLeaders = updatePartitionLeader();
        } catch (Exception e) {
            e.printStackTrace();
        }

        assert numSites > 0;
        assert numLeaders > 0;
        assert (numSites % numLeaders) == 0;

        this.numPartitions = numLeaders;
        // Initialize Volt hashinator.
        TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), TheHashinator.getConfigureBytes(this.numPartitions));
        return;
    }

    @Override
    public String getHostname(int pkey) {
        int partitionId = TheHashinator.getPartitionForParameter(
                VoltType.INTEGER, pkey);
        assert partitionId < this.numPartitions;
        assert partitionId >= 0;
        return this.partitionHostMap.get(partitionId);
    }

    @Override
    public Map<Integer, String> getPartitionHostMap() {
        return this.partitionHostMap;
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
        VoltTable hostMap = this.client.callProcedure("@QueryStats", sqlQuery).getResults()[0];

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

    // Update which host is the leader for each partition.
    // Return number of leaders in map on success, return -1 on failure.
    private int updatePartitionLeader() throws IOException, ProcCallException {
        // 1) Get the partitionID, leader mapping.
        VoltTable partitionLeaderMap = this.client.callProcedure("@Statistics", "TOPO", 0).getResults()[0];

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
