package org.dbos.apiary.cockroachdb;

import org.dbos.apiary.executor.ApiaryConnection;
import org.dbos.apiary.executor.FunctionOutput;
import org.dbos.apiary.interposition.ProvenanceBuffer;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CockroachDBConnection implements ApiaryConnection {
    private static final Logger logger = LoggerFactory.getLogger(CockroachDBConnection.class);

    private final PGSimpleDataSource ds;
    private final Connection connectionForPartitionInfo;
    private final ThreadLocal<Connection> connectionForFunction;
    private final String tableName;
    private final Map<String, Callable<CockroachDBFunction>> functions = new HashMap<>();
    private final Map<Integer, String> partitionHostMap = new HashMap<>();

    private class CockroachDBRange {
        public String startKey, endKey;
        public Integer leaseHolder;

        public CockroachDBRange(String startKey, String endKey, Integer leaseHolder) {
            this.startKey = startKey;
            this.endKey = endKey;
            this.leaseHolder = leaseHolder;
        }

    };

    private final ArrayList<CockroachDBRange> cockroachDBRanges = new ArrayList<>();

    public CockroachDBConnection(PGSimpleDataSource ds, String tableName) throws SQLException {
        this.ds = ds;
        this.tableName = tableName;

        this.connectionForPartitionInfo = ds.getConnection();
        this.connectionForFunction = ThreadLocal.withInitial(() -> {
            try {
                Connection conn = ds.getConnection();
                conn.setAutoCommit(false);
                return conn;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        });
        updatePartitionInfo();
    }

    public void registerFunction(String name, Callable<CockroachDBFunction> function) {
        functions.put(name, function);
    }

    public void deleteEntriesFromTable(String tableName) throws SQLException {
        logger.info(String.format("Deleting entries from table %s.", tableName));
        Connection conn = ds.getConnection();

        Statement deleteEntries = conn.createStatement();
        deleteEntries.execute(String.format("DELETE FROM %s WHERE 1=1;", tableName));
        deleteEntries.close();
    }

    public void seedKVTable(int numRows) throws SQLException {
        logger.info(String.format("Seeding data into KVTable."));
        Connection conn = ds.getConnection();
        conn.setAutoCommit(false);
        String insertSql = "UPSERT INTO KVTable VALUES (?, ?)";
        PreparedStatement pstmt = conn.prepareStatement(insertSql);
        int i = 0;
        while (i < numRows) {
            int batchSize = Math.min(10000, numRows - i + 1);
            for (int b = 0; b < batchSize; b++) {
                pstmt.setInt(1, (int) (Math.random() * Integer.MAX_VALUE));
                pstmt.setInt(2, (int) (Math.random() * Integer.MAX_VALUE));
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            i += batchSize;
        }
        conn.commit();
    }

    public void dropAndCreateTable(String tableName, String columnSpecStr) throws SQLException {
        logger.info(String.format("Dropping and creating table %s.", tableName));
        Connection conn = ds.getConnection();

        Statement dropTable = conn.createStatement();
        dropTable.execute(String.format("DROP TABLE IF EXISTS %s;", tableName));
        dropTable.close();

        Statement createTable = conn.createStatement();
        createTable.execute(String.format("CREATE TABLE %s%s;", tableName, columnSpecStr));
        createTable.close();
    }

    public Connection getConnectionForFunction() {
        return this.connectionForFunction.get();
    }

    @Override
    public FunctionOutput callFunction(ProvenanceBuffer provBuff, String service, long execID, long functionID, String name, Object... inputs) throws Exception {
        CockroachDBFunction function = functions.get(name).call();
        FunctionOutput f = null;
        try {
            f = function.apiaryRunFunction(new CockroachDBFunctionContext(provBuff, service, execID), inputs);
            connectionForFunction.get().commit();
        } catch (Exception e) {
            e.printStackTrace();
            connectionForFunction.get().rollback();
        }
        return f;
    }

    @Override
    public void updatePartitionInfo() {
        try {
            // Fill in `partitionHostMap`.
            partitionHostMap.clear();
            Statement getNodes = connectionForPartitionInfo.createStatement();
            ResultSet nodes = getNodes.executeQuery("select node_id, address from crdb_internal.gossip_nodes;");
            while (nodes.next()) {
                String[] ipAddrAndPort = nodes.getString("address").split(":");
                partitionHostMap.put(nodes.getInt("node_id"), /* ipAddr= */ipAddrAndPort[0]);
            }
            getNodes.close();

            // Fill in `cockroachDBRanges`.
            cockroachDBRanges.clear();
            Statement getRanges = connectionForPartitionInfo.createStatement();
            ResultSet ranges = getRanges.executeQuery(String.format("SHOW RANGES FROM table %s", tableName));
            while (ranges.next()) {
                cockroachDBRanges.add(new CockroachDBRange(ranges.getString("start_key"), ranges.getString("end_key"),
                        ranges.getInt("lease_holder")));
            }
            getRanges.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        logger.info("Updated partitionHostMap {}", partitionHostMap.entrySet());
        ArrayList<String> cockroachDBRangesStr = cockroachDBRanges.stream().map(range -> {
            return String.format("{[%s, %s], leaseholder=%d}", range.startKey, range.endKey, range.leaseHolder);
        }).collect(Collectors.toCollection(ArrayList<String>::new));
        logger.info("Updated cockroachDBRanges {}", cockroachDBRangesStr);

        return;
    }

    @Override
    public int getNumPartitions() {
        return partitionHostMap.size();
    }

    @Override
    public String getHostname(Object... input) {
        assert (input[0] instanceof String); // TODO: Support int type explicitly.
        String key = (String) input[0];

        // Linear search for now. In the future, we can sort and bin search or any other
        // efficient algo (in small scale, there aren't that many ranges).
        for (CockroachDBRange range : cockroachDBRanges) {
            Boolean satisfiesStart = range.startKey == null || (key.compareTo(range.startKey) >= 0);
            Boolean satisfiesEnd = range.endKey == null || (range.endKey.compareTo(key) >= 0);

            if (satisfiesStart && satisfiesEnd) {
                return partitionHostMap.get(range.leaseHolder);
            }
        }

        // If we can't find the range, something went wrong. Just log and fallback to
        // the first node.
        return partitionHostMap.get(0);
    }

    @Override
    public Map<Integer, String> getPartitionHostMap() {
        return partitionHostMap;
    }

}
