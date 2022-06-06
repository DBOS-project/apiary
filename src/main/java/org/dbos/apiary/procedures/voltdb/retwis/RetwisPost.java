package org.dbos.apiary.procedures.voltdb.retwis;

import org.dbos.apiary.voltdb.VoltContext;
import org.dbos.apiary.voltdb.VoltFunction;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class RetwisPost extends VoltFunction {
    public final SQLStmt addItem = new SQLStmt (
            "INSERT INTO RetwisPosts VALUES (?, ?, ?, ?);"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws Exception {
        return super.run(pkey, voltInput);
    }

    public int runFunction(VoltContext context, int userID, int postID, int timestamp, String post) {
        context.executeUpdate(addItem, userID, postID, timestamp, post);
        return userID;
    }

}
