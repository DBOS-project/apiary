package org.dbos.apiary.procedures.mysql;

import org.dbos.apiary.mysql.MysqlContext;
import org.dbos.apiary.mysql.MysqlFunction;
import org.dbos.apiary.utilities.ApiaryConfig;

public class MysqlReplacePerson extends MysqlFunction {

    private final static String nontxnUpdate = "UPDATE PersonTable SET Number=? WHERE Name=?;";

    public static int runFunction(MysqlContext context, String name, int number) throws Exception {
        if (ApiaryConfig.XDBTransactions) {
            context.executeUpsert("PersonTable", name, name, number);
        } else {
            context.executeUpdate(nontxnUpdate, number, name);
        }
        return number;
    }
}
