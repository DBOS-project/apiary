package org.dbos.apiary.procedures.postgres;

import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresFibSumFunction extends PostgresFunction {

    private final String addResult = "INSERT INTO KVTable(KVKey, KVValue) VALUES (?, ?) ON CONFLICT (KVKey) DO NOTHING;";

    public String runFunction(String key, String str1, String str2) {
        int num1 = Integer.parseInt(str1);
        int num2 = Integer.parseInt(str2);
        int sum = num1 + num2;
        context.apiaryExecuteUpdate(addResult, Integer.parseInt(key), sum);
        return String.valueOf(sum);
    }
}
