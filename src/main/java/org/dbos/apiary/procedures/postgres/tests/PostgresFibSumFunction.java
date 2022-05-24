package org.dbos.apiary.procedures.postgres.tests;

import org.dbos.apiary.function.ApiaryTransactionalContext;
import org.dbos.apiary.postgres.PostgresContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresFibSumFunction extends PostgresFunction {

    private static final String addResult = "INSERT INTO KVTable(KVKey, KVValue) VALUES (?, ?) ON CONFLICT (KVKey) DO NOTHING;";

    public static int runFunction(PostgresContext ctxt, int key, int num1, int num2) {
        ctxt.executeUpdate(addResult, key, num1 + num2);
        return num1 + num2;
    }
}
