package org.dbos.apiary.procedures.cockroachdb;

import org.dbos.apiary.cockroachdb.CockroachDBFunction;
import org.dbos.apiary.function.ApiaryTransactionalContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CockroachDBFibSumFunction extends CockroachDBFunction {

    private final PreparedStatement addResult;

    public CockroachDBFibSumFunction(Connection c) throws SQLException {
        this.addResult = c.prepareStatement("UPSERT INTO KVTable(KVKey, KVValue) VALUES (?, ?);");
    }

    public String runFunction(ApiaryTransactionalContext ctxt, String key, String str1, String str2) {
        int num1 = Integer.parseInt(str1);
        int num2 = Integer.parseInt(str2);
        int sum = num1 + num2;
        ctxt.apiaryExecuteUpdate(addResult, Integer.parseInt(key), sum);
        return String.valueOf(sum);
    }
}
