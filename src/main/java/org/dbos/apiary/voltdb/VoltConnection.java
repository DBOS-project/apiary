package org.dbos.apiary.voltdb;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.function.*;
import org.dbos.apiary.utilities.Utilities;
import org.dbos.apiary.function.WorkerContext;
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
import java.util.*;

import static org.dbos.apiary.utilities.ApiaryConfig.getApiaryClientID;

/**
 * For internal use only.
 */
public class VoltConnection implements ApiaryConnection {
    private static final Logger logger = LoggerFactory.getLogger(VoltConnection.class);
    public final Client client;
    public static String kPartitionInfoTableName = "PARTITIONINFO";
    private final Map<Integer, String> partitionHostMap = new HashMap<>();
    private final Map<Integer, String> hostIdNameMap = new HashMap<>();
    private int numPartitions;

    public VoltConnection(String hostname, Integer port) throws IOException {
        ClientConfig config = new ClientConfig();
        this.client = ClientFactory.createClient(config);
        client.createConnection(hostname, port);
        updatePartitionInfo();
    }

    private static VoltTable inputToVoltTable(String role, long execID, long functionID, Object... inputs) {
        int offset = 3;
        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[inputs.length+offset];
        columns[0] = new VoltTable.ColumnInfo("role", VoltType.STRING);
        columns[1] = new VoltTable.ColumnInfo("execID", VoltType.BIGINT);
        columns[2] = new VoltTable.ColumnInfo("functionID", VoltType.BIGINT);
        for (int i = 0; i < inputs.length; i++) {
            Object input = inputs[i];
            columns[i+offset] = VoltUtilities.objectToColumnInfo(i, input);
        }
        VoltTable v = new VoltTable(columns);
        Object[] row = new Object[v.getColumnCount()];
        row[0] = role;
        row[1] = execID;
        row[2] = functionID;
        for (int i = 0; i < inputs.length; i++) {
            Object input = inputs[i];
            if (input instanceof String[]) {
                row[i+offset] = Utilities.stringArraytoByteArray((String[]) input);
            } else if (input instanceof Integer) {
                row[i+offset] = input;
            } else if (input instanceof int[]) {
                row[i+offset] = Utilities.intArrayToByteArray((int[]) input);
            } else if (input instanceof String) {
                row[i+offset] = input;
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
        long functionID = inputRow.getLong(1);
        int offset = 2;
        Object[] input = new Object[voltInput.getColumnCount() - offset];

        int objIndex = 0;
        for (int i = offset; i < voltInput.getColumnCount(); i++, objIndex++) {
            String name = voltInput.getColumnName(i);
            if (name.startsWith("StringT")) {
                input[objIndex] = inputRow.getString(i);
            } else if (name.startsWith("StringArrayT")) {
                input[objIndex] = Utilities.byteArrayToStringArray(inputRow.getVarbinary(i));
            } else if (name.startsWith("IntegerT")) {
                input[objIndex] = (int) inputRow.getLong(i);
            } else if (name.startsWith("IntegerArrayT")) {
                input[objIndex] = Utilities.byteArrayToIntArray(inputRow.getVarbinary(i));
            } else if (name.startsWith("FutureT")) {
                long futureID = inputRow.getLong(i);
                input[objIndex] = new ApiaryFuture(futureID);
            } else if (name.startsWith("FutureArrayT")) {
                long[] futureIDs = Utilities.byteArrayToLongArray(inputRow.getVarbinary(i));
                ApiaryFuture[] futures = new ApiaryFuture[futureIDs.length];
                for (int j = 0; j < futures.length; j++) {
                    futures[j] = new ApiaryFuture(futureIDs[j]);
                }
                input[objIndex] = futures;
            } else {
                logger.error("Cannot support object type {}, index {}", name, objIndex);
                throw new IllegalArgumentException();
            }
        }
        // TODO: pass execution ID if needed.
        return new Task(0l, functionID, funcName, input);
    }

    @Override
    public FunctionOutput callFunction(String functionName, WorkerContext context, String role, long execID, long functionID,
                                       int replayMode, Object... inputs) throws IOException, ProcCallException {
        if (functionName.startsWith(getApiaryClientID)) {
            // Add input value for the procedure.
            inputs = new Integer[1];
            inputs[0] = 0;
        }
        VoltTable voltInput = inputToVoltTable(role, execID, functionID, inputs);
        assert (inputs[0] instanceof String || inputs[0] instanceof Integer);
        Integer keyInput = inputs[0] instanceof String ? Integer.parseInt((String) inputs[0]) : (int) inputs[0];
        VoltTable[] res = client.callProcedure(functionName, keyInput, voltInput).getResults();
        VoltTable retVal = res[0];
        assert (retVal.getColumnCount() == 1 && retVal.getRowCount() == 1);
        Object output = null;
        if (retVal.getColumnName(0).equals("stringOutput")) {
            output = retVal.fetchRow(0).getString(0);
        } else if (retVal.getColumnName(0).equals("intOutput")) {
            output = (int) retVal.fetchRow(0).getLong(0);
        } else if (retVal.getColumnName(0).equals("stringArrayOutput")) {
            output = Utilities.byteArrayToStringArray(retVal.fetchRow(0).getVarbinary(0));
        } else if (retVal.getColumnName(0).equals("intArrayOutput")) {
            output = Utilities.byteArrayToIntArray(retVal.fetchRow(0).getVarbinary(0));
        } else if (retVal.getColumnName(0).equals("futureOutput")) {
            long futureID = retVal.fetchRow(0).getLong(0);
            output = new ApiaryFuture(futureID);
        } else {
            logger.info("Invalid output {}", retVal);
        }
        List<Task> calledFunctions = new ArrayList<>();
        for (int i = 1; i < res.length; i++) {
            calledFunctions.add(voltOutputToTask(res[i]));
        }
        return new FunctionOutput(output, calledFunctions, "");
    }

    @Override
    public Set<TransactionContext> getActiveTransactions() {
        return null;
    }

    @Override
    public TransactionContext getLatestTransactionContext() {
        return null;
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

    private int getPartition(Object[] inputs) {
        assert (inputs[0] instanceof String || inputs[0] instanceof Integer);
        Integer keyInput = inputs[0] instanceof String ? Integer.parseInt((String) inputs[0]) : (int) inputs[0];
        int partitionId = TheHashinator.getPartitionForParameter(
                VoltType.INTEGER, keyInput);
        assert partitionId < this.numPartitions;
        assert partitionId >= 0;
        return partitionId;
    }

    @Override
    public String getHostname(Object... input) {
        return this.partitionHostMap.get(getPartition(input));
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
