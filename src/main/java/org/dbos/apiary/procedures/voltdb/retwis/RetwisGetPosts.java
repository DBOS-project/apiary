package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.interposition.ApiaryStatefulFunctionContext;
import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class RetwisGetPosts extends VoltApiaryProcedure {
    public final SQLStmt getPosts = new SQLStmt(
            "SELECT Post FROM RetwisPosts WHERE UserID=? ORDER BY Timestamp LIMIT 10;"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(voltInput);
    }

    public String runFunction(ApiaryStatefulFunctionContext context, String userID) {
        VoltTable result = ((VoltTable[]) context.apiaryExecuteQuery(getPosts, Integer.parseInt(userID)))[0];
        StringBuilder posts = new StringBuilder();
        String sep = "";
        while (result.advanceRow()) { // TODO: Use properly escaped JSON or something.
            posts.append(sep);
            posts.append(result.getString(0));
            sep = ",";
        }
        return posts.toString();
    }
}
