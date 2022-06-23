package org.dbos.apiary.mysql;

import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MysqlContext extends ApiaryContext {
    private static final Logger logger = LoggerFactory.getLogger(MysqlContext.class);
    public static final String apiaryID = "__apiaryID__";
    public static final String beginVersion = "__beginVersion__";
    public static final String endVersion = "__endVersion__";

    private final Connection conn;

    private final TransactionContext txc;

    Map<String, List<String>> writtenKeys = new HashMap<>();

    public MysqlContext(Connection conn, WorkerContext workerContext, TransactionContext txc, String service, long execID, long functionID) {
        super(workerContext, service, execID, functionID);
        this.conn = conn;
        this.txc = txc;
    }

    private void prepareStatement(PreparedStatement ps, Object[] input) throws SQLException {
        for (int i = 0; i < input.length; i++) {
            Object o = input[i];
            if (o instanceof Integer) {
                ps.setInt(i + 1, (Integer) o);
            } else if (o instanceof String) {
                ps.setString(i + 1, (String) o);
            } else {
                assert (false); // TODO: More types.
            }
        }
    }

    public void executeUpsert(String tableName, String id, Object... input) throws Exception {
        // TODO: This interface is not the natural SQL interface. Figure out a better one? E.g., can we support arbitrary update queries?
        StringBuilder query = new StringBuilder(String.format("INSERT INTO %s VALUES (?, ?, ?", tableName));
        for (int i = 0; i < input.length; i++) {
            query.append(", ?");
        }
        query.append(");");
        logger.info(query.toString());
        PreparedStatement pstmt = conn.prepareStatement(query.toString());
        Object[] apiaryInput = new Object[input.length + 3];
        apiaryInput[0] = id;
        apiaryInput[1] = txc.txID;
        apiaryInput[2] = Long.MAX_VALUE;
        System.arraycopy(input, 0, apiaryInput, 3, input.length);
        prepareStatement(pstmt, apiaryInput);
        pstmt.executeUpdate();
    }

    public ResultSet executeQuery(String procedure, Object... input) throws Exception {
        // TODO: This implementation assume predicates at the end. No more group by or others. May find a better solution.
        // Also hard to use prepared statement because the number of active transactions varies.
        String sanitizeQuery = procedure.replaceAll(";+$", "");
        StringBuilder filterQuery = new StringBuilder(sanitizeQuery);
        String activeTxnString = txc.activeTransactions.stream().map(Object::toString).collect(Collectors.joining(","));
        // Add filters to the end.
        filterQuery.append(String.format(" AND %s < %d ", beginVersion, txc.xmax));
        if (!activeTxnString.isEmpty()) {
            filterQuery.append(String.format(" AND %s NOT IN (%s) ", beginVersion, activeTxnString));
        }
        filterQuery.append(String.format(" AND ( %s >= %d ", endVersion, txc.xmax));
        if (!activeTxnString.isEmpty()) {
            filterQuery.append(String.format(" OR %s IN (%s) ", endVersion, activeTxnString));
        }

        filterQuery.append("); ");
        logger.info(filterQuery.toString());

        PreparedStatement pstmt = conn.prepareStatement(filterQuery.toString());
        prepareStatement(pstmt, input);
        ResultSet rs = pstmt.executeQuery();

        return rs;
    }

    @Override
    public FunctionOutput apiaryCallFunction(String name, Object... inputs) throws Exception {
        // TODO: implement.
        return null;
    }
}
