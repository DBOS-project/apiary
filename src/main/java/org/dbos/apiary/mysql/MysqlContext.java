package org.dbos.apiary.mysql;

import com.mysql.cj.jdbc.exceptions.MySQLTransactionRollbackException;
import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.utilities.Percentile;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class MysqlContext extends ApiaryContext {  
    private static final Logger logger = LoggerFactory.getLogger(MysqlContext.class);
    public static final String apiaryID = "__apiaryID__";
    public static final String beginVersion = "__beginVersion__";
    public static final String endVersion = "__endVersion__";

    Percentile upserts = null;
    Percentile queries = null;
    private final Connection conn;

    private final TransactionContext txc;

    private final Map<String, Map<String, AtomicBoolean>> lockManager;

    private boolean mysqlUpdated = false;
    Map<String, List<String>> writtenKeys;

    public MysqlContext(Connection conn, Map<String, List<String>> writtenKeys, Map<String, Map<String, AtomicBoolean>> lockManager,  WorkerContext workerContext, TransactionContext txc, String service, long execID, long functionID, Percentile upserts, Percentile queries) {
        super(workerContext, service, execID, functionID, false);
        this.writtenKeys = writtenKeys;
        this.conn = conn;
        this.txc = txc;
        this.upserts = upserts;
        this.queries = queries;
        this.lockManager = lockManager;
    }

    private void prepareStatement(PreparedStatement ps, Object[] input) throws SQLException {
        for (int i = 0; i < input.length; i++) {
            Object o = input[i];
            if (o instanceof Integer) {
                ps.setInt(i + 1, (Integer) o);
            } else if (o instanceof String) {
                ps.setString(i + 1, (String) o);
            } else if (o instanceof Long)  {
                ps.setLong(i + 1, (Long) o);
            } else if (o instanceof Float)  {
                ps.setFloat(i + 1, (Float) o);
            } else if (o instanceof Double) {
                ps.setDouble(i + 1, (Double) o);
            } else if (o instanceof Timestamp) {
                ps.setTimestamp(i + 1, (Timestamp) o);
            } else {
                logger.info("type {} for input {} not recogized ", o.toString(), i);
                assert (false); // TODO: More types.
            }
        }
    }

    // Note: this executeUpdate should only be used by non-XDST transactions.
    // XDST transactions must use executeUpsert.
    public void executeUpdate(String procedure, Object... input) throws Exception {
        assert (!ApiaryConfig.XDBTransactions);
        PreparedStatement pstmt = conn.prepareStatement(procedure);
        prepareStatement(pstmt, input);
        pstmt.executeUpdate();
    }

    public void executeUpsert(String tableName, String id, Object... input) throws Exception {
        long t0 = System.nanoTime();
        if (!ApiaryConfig.XDBTransactions) {
            StringBuilder query = new StringBuilder(String.format("INSERT INTO %s VALUES (", tableName));
            for (int i = 0; i < input.length; i++) {
                if (i == 0) {
                    query.append("?");
                } else {
                    query.append(", ?");
                }
            }
            query.append(");");
            PreparedStatement pstmt = conn.prepareStatement(query.toString());
            prepareStatement(pstmt, input);
            pstmt.executeUpdate();
            Long time = System.nanoTime() - t0;
            upserts.add(time / 1000);
            return;
        }

        // Grab write lock for the upsert.
        lockManager.putIfAbsent(tableName, new ConcurrentHashMap<>());
        lockManager.get(tableName).putIfAbsent(id, new AtomicBoolean(false));
        boolean available = lockManager.get(tableName).get(id).compareAndSet(false, true);
        if (!available) {
            throw new PSQLException("MySQL tuple locked", PSQLState.SERIALIZATION_FAILURE);
        }

        // TODO: This interface is not the natural SQL interface. Figure out a better one? E.g., can we support arbitrary update queries?
        StringBuilder query = new StringBuilder(String.format("INSERT INTO %s VALUES (?, ?, ?", tableName));
        for (int i = 0; i < input.length; i++) {
            query.append(", ?");
        }
        query.append(");");

        writtenKeys.putIfAbsent(tableName, new ArrayList<>());
        writtenKeys.get(tableName).add(id);
        PreparedStatement pstmt = conn.prepareStatement(query.toString());
        Object[] apiaryInput = new Object[input.length + 3];
        apiaryInput[0] = id;
        apiaryInput[1] = txc.txID;
        apiaryInput[2] = Long.MAX_VALUE;
        System.arraycopy(input, 0, apiaryInput, 3, input.length);
        prepareStatement(pstmt, apiaryInput);
        pstmt.executeUpdate();

        // make writes visible
        String updateVisibility = String.format("UPDATE %s SET %s = ? WHERE %s = ? AND %s < ? AND %s = ?", tableName, MysqlContext.endVersion, MysqlContext.apiaryID, MysqlContext.beginVersion, MysqlContext.endVersion);
        // UPDATE table SET __endVersion__ = ? WHERE __apiaryID__ = ? and __beginVersion__ < ? and __endVersion__ == infinity

        while (true) {
            try {
                pstmt = conn.prepareStatement(updateVisibility);
                prepareStatement(pstmt, new Object[]{txc.txID, id, txc.txID, Long.MAX_VALUE});
                pstmt.executeUpdate();
            } catch (MySQLTransactionRollbackException m) {
                if (m.getErrorCode() == 1213 || m.getErrorCode() == 1205) {
                    continue; // Deadlock or lock timed out
                } else {
                    m.printStackTrace();
                    logger.error("2. Failed to update valid txn {}", txc.txID);
                    logger.info("2. Validate update query: {}", query);
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("3. Failed to update valid txn {}", txc.txID);
                logger.info("3. Validate update query: {}", query);
            }
            break;
        }

        mysqlUpdated = true;

        Long time = System.nanoTime() - t0;
        upserts.add(time / 1000);
    }

    public ResultSet executeQuery(String procedure, Object... input) throws Exception {
        long t0 = System.nanoTime();
        ResultSet rs;
        String sanitizeQuery = procedure.replaceAll(";+$", "");
        if (!ApiaryConfig.XDBTransactions) {
            PreparedStatement pstmt = conn.prepareStatement(procedure);
            prepareStatement(pstmt, input);
            rs = pstmt.executeQuery();
            Long time = System.nanoTime() - t0;
            queries.add(time / 1000);
            return rs;
        }

        // TODO: This implementation assume predicates at the end. No more group by or others. May find a better solution.
        // Also hard to use prepared statement because the number of active transactions varies.
        StringBuilder filterQuery = new StringBuilder(sanitizeQuery);
        String activeTxnString = txc.activeTransactions.stream().map(Object::toString).collect(Collectors.joining(","));
        // Add filters to the end.
        filterQuery.append(String.format(" AND (( %s < %d ", beginVersion, txc.xmax));
        if (!activeTxnString.isEmpty()) {
            filterQuery.append(String.format(" AND %s NOT IN (%s) ) ", beginVersion, activeTxnString));
        } else {
            filterQuery.append(" )");
        }
        // If it has updates, then need to read its own writes.
        if (mysqlUpdated) {
            filterQuery.append(String.format(" OR %s = %d )", beginVersion, txc.txID));
        } else {
            filterQuery.append(")");
        }

        filterQuery.append(String.format(" AND ( %s >= %d ", endVersion, txc.xmax));
        if (!activeTxnString.isEmpty()) {
            filterQuery.append(String.format(" OR %s IN (%s) )", endVersion, activeTxnString));
        } else {
            filterQuery.append(" )");
        }

        // If it has updates, then need to read its own writes.
        if (mysqlUpdated) {
            filterQuery.append(String.format(" AND %s != %d ", endVersion, txc.txID));
        }

        filterQuery.append(" ;");

        PreparedStatement pstmt = conn.prepareStatement(filterQuery.toString());
        prepareStatement(pstmt, input);

        rs = pstmt.executeQuery();

        Long time = System.nanoTime() - t0;
        queries.add(time / 1000);
        return rs;
    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) throws Exception {
        // TODO: implement.
        return null;
    }
}
