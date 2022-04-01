package org.dbos.apiary.procedures.voltdb;

import org.dbos.apiary.interposition.ApiaryFuture;
import org.dbos.apiary.voltdb.VoltApiaryProcedure;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

import java.lang.reflect.InvocationTargetException;

import static org.dbos.apiary.utilities.ApiaryConfig.defaultPkey;

public class ImagenetInferenceFunction extends VoltApiaryProcedure {

    public final SQLStmt getData = new SQLStmt(
            "SELECT * FROM ImagenetData WHERE ID=?;"
    );

    public VoltTable[] run(int pkey, VoltTable voltInput) throws InvocationTargetException, IllegalAccessException {
        return super.run(pkey, voltInput);
    }

    public ApiaryFuture runFunction(String stringId) {

        int id = Integer.parseInt(stringId);
        
        // Grab data and extract from VoltTable
        VoltTable res = ((VoltTable[]) funcApi.apiaryExecuteQuery(getData, id))[0];

        VoltTableRow row = res.fetchRow(0);
        byte[] data = row.getVarbinary("IMAGE");
        String dataString = new String(data);

        // Queue stateless external function
        ApiaryFuture classifications = funcApi.apiaryQueueFunction("infer", defaultPkey, dataString);
        
        // Queue insertion back into DB
        funcApi.apiaryQueueFunction("ImagenetInsertFunction", defaultPkey, classifications);
        
        return classifications;
    }
}
