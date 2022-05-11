package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.function.ApiaryTransactionalContext;
import org.dbos.apiary.voltdb.VoltFunction;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class RetwisFollow extends VoltFunction {
    public final SQLStmt addItem = new SQLStmt (
            "INSERT INTO RetwisFollowees VALUES (?, ?);"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public int runFunction(ApiaryTransactionalContext context, int userID, int followeeID) {
        context.apiaryExecuteUpdate(addItem, userID, followeeID);
        return userID;
    }
}
