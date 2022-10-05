package org.dbos.apiary.procedures.mysql;

import org.dbos.apiary.mysql.MysqlContext;
import org.dbos.apiary.mysql.MysqlFunction;

import java.util.ArrayList;
import java.util.List;

public class MysqlBulkAddPerson extends MysqlFunction {

    public static int runFunction(MysqlContext context, String[] names, int[] numbers) throws Exception {
        List<String> ids = new ArrayList<>();
        List<Object[]> inputs = new ArrayList<>();
        for (int i = 0; i < names.length; i++) {
            Object[] input = new Object[2];
            input[0] = names[i];
            input[1] = numbers[i];
            inputs.add(input);
            ids.add(names[i]);
        }

        context.insertMany("PersonTable", ids, inputs);
        return ids.size();
    }
}
