package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class RetwisGetTimeline extends VoltApiaryProcedure {
    public final SQLStmt getFollowees = new SQLStmt(
            "SELECT FolloweeID FROM RetwisFollowees WHERE UserID=?;"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(voltInput);
    }

    public ApiaryFuture runFunction(ApiaryStatefulFunctionContext context, String userIDString) {
        int userID = Integer.parseInt(userIDString);
        VoltTable followeesTable = ((VoltTable[]) context.apiaryExecuteQuery(getFollowees, userID))[0];
        List<Integer> followeesList = new ArrayList<>();
        while(followeesTable.advanceRow()) {
            followeesList.add((int) followeesTable.getLong(0));
        }
        ApiaryFuture[] futures = new ApiaryFuture[followeesList.size()];
        for (int i = 0; i < followeesList.size(); i++) {
            futures[i] = context.apiaryQueueFunction("RetwisGetPosts", String.valueOf(followeesList.get(i)));
        }
        return context.apiaryQueueFunction("RetwisMerge", (Object) futures);
    }
}
