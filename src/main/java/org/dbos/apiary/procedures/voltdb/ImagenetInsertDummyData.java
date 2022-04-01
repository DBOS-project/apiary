package org.dbos.apiary.procedures.voltdb;

import java.util.Random;

import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class ImagenetInsertDummyData extends VoltApiaryProcedure {

    public final SQLStmt insertData = new SQLStmt(
            // PKEY, ID, IMAGE
            "UPSERT INTO ImagenetData VALUES (?, ?, ?);"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public String runFunction(String dummyInput) {

        // TODO
        byte[] data = new byte[224*224*3];
        new Random().nextBytes(data);

        for (int i = 0; i < 200; i++) {
            funcApi.apiaryExecuteUpdate(insertData, 0, i, data);
        }

        return "Successfully inserted data.";
    }
}
