package org.dbos.apiary.postgres;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgresFunctionContext extends ApiaryStatefulFunctionContext {
    // This connection ties to all prepared statements in one transaction.
    private final Connection conn;

    public PostgresFunctionContext(Connection c) { this.conn= c; }

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

    @Override
    protected void internalExecuteUpdate(Object procedure, Object... input) {
        try {
            // First, prepare statement. Then, execute.
            PreparedStatement pstmt = conn.prepareStatement((String) procedure);
            prepareStatement(pstmt, input);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Object internalExecuteQuery(Object procedure, Object... input) {
        try {
            // First, prepare statement. Then, execute.
            PreparedStatement pstmt = conn.prepareStatement((String) procedure);
            prepareStatement(pstmt, input);
            return pstmt.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
}
