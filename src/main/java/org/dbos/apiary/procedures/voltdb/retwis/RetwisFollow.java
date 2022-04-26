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
        return super.run(voltInput);
    }

    public String runFunction(ApiaryStatefulFunctionContext context, String userID, String followeeID) {
        context.apiaryExecuteUpdate(addItem, Integer.parseInt(userID), Integer.parseInt(followeeID));
        return userID;
    }
}
