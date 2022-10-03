package org.dbos.apiary.procedures.mysql;

import org.dbos.apiary.mysql.MysqlContext;
import org.dbos.apiary.mysql.MysqlFunction;
import org.dbos.apiary.utilities.ApiaryConfig;

public class MysqlUpsertPerson extends MysqlFunction {

    private final static String nontxnUpdate = "INSERT INTO PersonTable VALUES (?, ?) ON DUPLICATE KEY UPDATE Number=VALUES(Number);";
    public static int runFunction(MysqlContext context, String name, int number) throws Exception {
        if (ApiaryConfig.XDBTransactions) {
            context.executeUpsert("PersonTable", name, name, number);
        } else {
            context.executeUpdate(nontxnUpdate, name, number);
        }
        return number;
    }
}
