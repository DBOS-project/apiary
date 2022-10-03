package org.dbos.apiary.procedures.mysql;

import org.dbos.apiary.mysql.MysqlContext;
import org.dbos.apiary.mysql.MysqlFunction;

import java.sql.ResultSet;

public class MysqlWriteReadPerson extends MysqlFunction {
    // Test read your own writes.

    private final static String find = "SELECT Number FROM PersonTable WHERE Name=? ;";

    public static int runFunction(MysqlContext context, String name, int number) throws Exception {
        context.executeUpsert("PersonTable", name, name, number);

        ResultSet rs = context.executeQuery(find, name);
        int count = -1;  // Default not found.
        if (rs.next()) {
            count = rs.getInt(1);
            assert (!rs.next());  // Should only have at most 1 row.
        }
        return count;
    }
}
