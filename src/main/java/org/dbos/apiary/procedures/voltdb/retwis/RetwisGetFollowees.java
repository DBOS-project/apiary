package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class RetwisGetFollowees extends VoltApiaryProcedure {
    public final SQLStmt getFollowees = new SQLStmt(
            "SELECT FolloweeID FROM RetwisFollowees WHERE UserID=?;"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(voltInput);
    }

    public int[] runFunction(ApiaryStatefulFunctionContext context, int userID) {
        VoltTable followeesTable = ((VoltTable[]) context.apiaryExecuteQuery(getFollowees, userID))[0];
        List<Integer> followees = new ArrayList<>();
        while (followeesTable.advanceRow()) {
            followees.add((int) followeesTable.getLong(0));
        }
        return followees.stream().mapToInt(i -> i).toArray();
    }
}
