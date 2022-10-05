package org.dbos.apiary.procedures.mysql;

import org.dbos.apiary.mysql.MysqlContext;
import org.dbos.apiary.mysql.MysqlFunction;
import org.dbos.apiary.utilities.ApiaryConfig;

import java.sql.ResultSet;

public class MysqlWriteReadPerson extends MysqlFunction {
    // Test read your own writes.
    private final static String nontxnUpdate = "INSERT INTO PersonTable VALUES (?, ?) ON DUPLICATE KEY UPDATE Number=VALUES(Number);";
    private final static String find = "SELECT Number FROM PersonTable WHERE Name=? ;";

    public static int runFunction(MysqlContext context, String name, int number) throws Exception {
        if (ApiaryConfig.XDBTransactions) {
            context.executeUpsert("PersonTable", name, name, number);
        } else {
            context.executeUpdate(nontxnUpdate, name, number);
        }

        ResultSet rs = context.executeQuery(find, name);
        int count = -1;  // Default not found.
        if (rs.next()) {
            count = rs.getInt(1);
            assert (!rs.next());  // Should only have at most 1 row.
        }
        rs.close();
        return count;
    }
}
