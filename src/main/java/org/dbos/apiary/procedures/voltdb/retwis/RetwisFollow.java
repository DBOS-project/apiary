package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class RetwisFollow extends VoltApiaryProcedure {
    public final SQLStmt addItem = new SQLStmt (
            "INSERT INTO RetwisFollowers VALUES (?, ?, ?);"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public String runFunction(String userID, String followeeID) {
        funcApi.apiaryExecuteUpdate(addItem, Integer.parseInt(userID), Integer.parseInt(userID), Integer.parseInt(followeeID));
        return userID;
    }
}
