package org.dbos.apiary.procedures.mysql;

import org.dbos.apiary.mysql.MysqlContext;
import org.dbos.apiary.mysql.MysqlFunction;

import java.sql.ResultSet;

public class MysqlQueryPerson extends MysqlFunction {
    private final static String find = "SELECT COUNT(*) FROM PersonTable WHERE Name=? ;";

    public static int runFunction(MysqlContext context, String name) throws Exception {
        ResultSet rs = context.executeQuery(find, name);
        int count = 0;
        if (rs.next()) {
            count = rs.getInt(1);
        }
        return count;
    }
}
