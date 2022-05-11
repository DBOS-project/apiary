package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.function.ApiaryTransactionalContext;
import org.dbos.apiary.voltdb.VoltFunction;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class RetwisGetPosts extends VoltFunction {
    public final SQLStmt getPosts = new SQLStmt(
            "SELECT Post FROM RetwisPosts WHERE UserID=? ORDER BY Timestamp LIMIT 10;"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public String runFunction(ApiaryTransactionalContext context, int userID) {
        VoltTable result = ((VoltTable[]) context.apiaryExecuteQuery(getPosts, userID))[0];
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
