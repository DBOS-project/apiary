package org.dbos.apiary.procedures.sqlite;

import org.dbos.apiary.sqlite.SQLiteFunction;
import org.dbos.apiary.sqlite.SQLiteFunctionContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SQLiteFibSumFunction extends SQLiteFunction {

    private final PreparedStatement addResult;

    public SQLiteFibSumFunction(Connection c) throws SQLException {
        this.addResult = c.prepareStatement("INSERT INTO KVTable(KVKey, KVValue) VALUES (?, ?);");
    }

    public String runFunction(String key, String str1, String str2) {
        int num1 = Integer.parseInt(str1);
        int num2 = Integer.parseInt(str2);
        int sum = num1 + num2;
        context.apiaryExecuteUpdate(addResult, Integer.parseInt(key), sum);
        return String.valueOf(sum);
    }
}
