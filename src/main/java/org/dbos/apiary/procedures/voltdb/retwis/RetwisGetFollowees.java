package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class RetwisGetFollowees extends VoltApiaryProcedure {
    public final SQLStmt getFollowees = new SQLStmt(
            "SELECT FolloweeID FROM RetwisFollowees WHERE UserID=?;"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(voltInput);
    }

    public String runFunction(ApiaryStatefulFunctionContext context, String userIDString) {
        int userID = Integer.parseInt(userIDString);
        VoltTable followeesTable = ((VoltTable[]) context.apiaryExecuteQuery(getFollowees, userID))[0];
        StringBuilder followees = new StringBuilder();
        String sep = "";
        while(followeesTable.advanceRow()) {
            long followee = followeesTable.getLong(0);
            followees.append(sep);
            followees.append(followee);
            sep = ",";
        }
        return followees.toString();
    }
}
