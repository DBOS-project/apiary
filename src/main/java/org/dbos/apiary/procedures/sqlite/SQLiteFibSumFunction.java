package org.dbos.apiary.procedures.sqlite;

import org.dbos.apiary.sqlite.SQLiteFunctionInterface;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.dbos.apiary.utilities.ApiaryConfig.defaultPkey;

public class SQLiteFibSumFunction extends SQLiteFunctionInterface {

    private final PreparedStatement addResult;

    public SQLiteFibSumFunction(Connection c) throws SQLException {
        this.addResult = c.prepareStatement("INSERT INTO KVTable(pkey, KVKey, KVValue) VALUES (?, ?, ?);");
    }

    public String runFunction(String key, String str1, String str2) {
        int num1 = Integer.parseInt(str1);
        int num2 = Integer.parseInt(str2);
        int sum = num1 + num2;
        this.apiaryExecuteUpdate(addResult, defaultPkey, Integer.parseInt(key), sum);
        return String.valueOf(sum);
    }
}
