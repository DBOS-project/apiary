package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.function.ApiaryFuture;
import org.dbos.apiary.voltdb.VoltContext;
import org.dbos.apiary.voltdb.VoltFunction;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class RetwisGetTimeline extends VoltFunction {
    public final SQLStmt getFollowees = new SQLStmt(
            "SELECT FolloweeID FROM RetwisFollowees WHERE UserID=?;"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws Exception {
        return super.run(pkey, voltInput);
    }

    public ApiaryFuture runFunction(VoltContext context, int userID) {
        VoltTable followeesTable = (context.executeQuery(getFollowees, userID))[0];
        List<Integer> followeesList = new ArrayList<>();
        while(followeesTable.advanceRow()) {
            followeesList.add((int) followeesTable.getLong(0));
        }
        ApiaryFuture[] futures = new ApiaryFuture[followeesList.size()];
        for (int i = 0; i < followeesList.size(); i++) {
            futures[i] = context.apiaryQueueFunction("RetwisGetPosts", followeesList.get(i));
        }
        return context.apiaryQueueFunction("RetwisMerge", (Object) futures);
    }
}
