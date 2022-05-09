package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class RetwisFollow extends VoltApiaryProcedure {
    public final SQLStmt addItem = new SQLStmt (
            "INSERT INTO RetwisFollowees VALUES (?, ?);"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public int runFunction(ApiaryStatefulFunctionContext context, int userID, int followeeID) {
        context.apiaryExecuteUpdate(addItem, userID, followeeID);
        return userID;
    }
}
