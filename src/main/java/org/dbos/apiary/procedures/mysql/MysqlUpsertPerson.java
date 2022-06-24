package org.dbos.apiary.procedures.mysql;

import org.dbos.apiary.mysql.MysqlContext;
import org.dbos.apiary.mysql.MysqlFunction;

public class MysqlUpsertPerson extends MysqlFunction {

    public static int runFunction(MysqlContext context, String name, int number) throws Exception {
        context.executeUpsert("PersonTable", name, name, number);
        return number;
    }
}
