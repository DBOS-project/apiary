package org.dbos.apiary.procedures.voltdb;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import java.lang.reflect.InvocationTargetException;

import static org.dbos.apiary.utilities.ApiaryConfig.defaultPkey;

public class InferenceFunction extends VoltApiaryProcedure {

    public final SQLStmt getData = new SQLStmt(
            "SELECT * FROM MnistData ORDER BY ID;"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public ApiaryFuture runFunction(String keyString) {
        
        // Grab data and extract from VoltTable
        VoltTable res = ((VoltTable[]) funcApi.apiaryExecuteQuery(getData))[0];

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < res.getRowCount(); i++) {
            String image = res.fetchRow(i).getString("IMAGE");
            sb.append(image);
            sb.append("&");
        }

        // Remove last &
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }

        String data = sb.toString();
        
        // Queue stateless external function
        ApiaryFuture classifications = funcApi.apiaryQueueFunction("infer", defaultPkey, data);
        
        // Queue insertion back into DB
        funcApi.apiaryQueueFunction("InsertMnistFunction", defaultPkey, classifications);
        
        return classifications;
    }
}
