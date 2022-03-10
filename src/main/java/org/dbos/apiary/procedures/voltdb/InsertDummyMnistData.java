package org.dbos.apiary.procedures.voltdb;

import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

public class InsertDummyMnistData extends VoltApiaryProcedure {

    public final SQLStmt insertData = new SQLStmt(
            // PKEY, ID, IMAGE
            "UPSERT INTO MnistData VALUES (?, ?, ?);"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public String runFunction(String dummyInput) {

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < 784; i++) {
            // Pick a number idk
            int character;
            if (i / 28 < 5 || 22 < i / 28) {
                character = 0;
            } else if (i % 28 < 5 || 22 < i % 28) {
                character = 0;
            } else {
                character = (i * 37) % 256;
            }

            sb.append(String.valueOf(character));
            sb.append(",");
        }

        // Remove last ,
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }

        String data = sb.toString();

        for (int i = 0; i < 200; i++) {
            funcApi.apiaryExecuteUpdate(insertData, 0, i, data);
        }

        return "Successfully inserted data.";
    }
}
