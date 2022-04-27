package org.dbos.apiary.procedures.postgres;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.postgres.PostgresFunction;

public class PostgresFibSumFunction extends PostgresFunction {

    private static final String addResult = "INSERT INTO KVTable(KVKey, KVValue) VALUES (?, ?) ON CONFLICT (KVKey) DO NOTHING;";

    public static int runFunction(ApiaryStatefulFunctionContext ctxt, int key, int num1, int num2) {
        ctxt.apiaryExecuteUpdate(addResult, key, num1 + num2);
        return num1 + num2;
    }
}
