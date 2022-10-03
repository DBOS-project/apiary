package org.dbos.apiary.procedures.mysql;

import org.dbos.apiary.mysql.MysqlContext;
import org.dbos.apiary.mysql.MysqlFunction;

import java.sql.ResultSet;

public class MysqlQueryPerson extends MysqlFunction {
    private final static String find = "SELECT Number FROM PersonTable WHERE Name=? ;";

    public static int runFunction(MysqlContext context, String name) throws Exception {
        ResultSet rs = context.executeQuery(find, name);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();
        return count;
    }
}
